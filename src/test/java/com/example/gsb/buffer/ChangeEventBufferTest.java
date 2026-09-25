package com.example.gsb.buffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ChangeEventBufferTest {

    private static final String KEY_A = "order-1";
    private static final String KEY_B = "user-7";

    private final ChangeEventBuffer buffer = new ChangeEventBuffer(3);

    private static List<Long> drainSequences(ChangeEventBuffer buffer, String key) {
        List<Long> sequences = new ArrayList<>();
        Optional<ChangeEvent> event;
        while ((event = buffer.poll(key)).isPresent()) {
            sequences.add(event.get().sequence());
            buffer.ack(event.get().sequence());
        }
        return sequences;
    }

    @Test
    @DisplayName("顺序保证：全局序号连续不跳号，同一聚合键严格按序号产出")
    void globalSequenceIsContiguousAndPerKeyOrderIsStrict() {
        List<Long> assigned = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            String key = i % 2 == 0 ? KEY_A : KEY_B;
            assigned.add(buffer.append(key, "payload-" + i).sequence());
        }
        assertThat(assigned).containsExactly(1L, 2L, 3L, 4L, 5L, 6L);

        assertThat(drainSequences(buffer, KEY_A)).containsExactly(1L, 3L, 5L);
        assertThat(drainSequences(buffer, KEY_B)).containsExactly(2L, 4L, 6L);
    }

    @Test
    @DisplayName("分区并行：一个分区有在途事件时不阻塞其它分区")
    void partitionsAreConsumedIndependently() {
        buffer.append(KEY_A, "a1");
        buffer.append(KEY_B, "b1");
        buffer.append(KEY_A, "a2");

        Optional<ChangeEvent> a1 = buffer.poll(KEY_A);
        assertThat(a1).isPresent();

        // KEY_A 有在途事件，再次 poll 返回空
        assertThat(buffer.poll(KEY_A)).isEmpty();
        // 但 KEY_B 不受影响，可并行消费
        assertThat(buffer.poll(KEY_B)).hasValueSatisfying(e -> assertThat(e.sequence()).isEqualTo(2L));

        buffer.ack(1L);
        assertThat(buffer.poll(KEY_A)).hasValueSatisfying(e -> assertThat(e.sequence()).isEqualTo(3L));
    }

    @Test
    @DisplayName("失败隔离：重试超限进入死信，不阻塞同键后续事件与其它分区")
    void failedEventIsRetriedThenQuarantinedWithoutBlockingOthers() {
        ChangeEventBuffer buffer = new ChangeEventBuffer(2);
        buffer.append(KEY_A, "a1");
        buffer.append(KEY_A, "a2");
        buffer.append(KEY_B, "b1");

        ChangeEvent a1 = buffer.poll(KEY_A).orElseThrow();
        buffer.fail(a1.sequence());
        // 未达上限：同键重投同一事件
        assertThat(buffer.poll(KEY_A)).hasValueSatisfying(e -> assertThat(e.sequence()).isEqualTo(1L));
        buffer.fail(1L);

        // 达到上限：进入隔离区
        assertThat(buffer.deadLetters()).extracting(ChangeEvent::sequence).containsExactly(1L);
        assertThat(buffer.stats().quarantinedCount()).isEqualTo(1);

        // 同键后续事件继续产出，其它分区也正常
        assertThat(buffer.poll(KEY_A)).hasValueSatisfying(e -> assertThat(e.sequence()).isEqualTo(2L));
        assertThat(buffer.poll(KEY_B)).hasValueSatisfying(e -> assertThat(e.sequence()).isEqualTo(3L));
    }

    @Test
    @DisplayName("位点提交：确认后缓冲被清理，最老未确认序号随之前移")
    void ackCommitsPositionAndCleansUpBuffer() {
        buffer.append(KEY_A, "a1");
        buffer.append(KEY_A, "a2");
        buffer.append(KEY_A, "a3");

        assertThat(buffer.stats().backlog()).isEqualTo(3);
        assertThat(buffer.stats().oldestUnackedSequence()).isEqualTo(1);
        assertThat(buffer.committedPosition()).isZero();

        buffer.ack(buffer.poll(KEY_A).orElseThrow().sequence());
        assertThat(buffer.stats().backlog()).isEqualTo(2);
        assertThat(buffer.stats().oldestUnackedSequence()).isEqualTo(2);
        assertThat(buffer.committedPosition()).isEqualTo(1);

        // 已清理的位点不可重放
        assertThatThrownBy(() -> buffer.replayFrom(1L))
                .isInstanceOf(IllegalArgumentException.class);

        buffer.ack(buffer.poll(KEY_A).orElseThrow().sequence());
        buffer.ack(buffer.poll(KEY_A).orElseThrow().sequence());
        assertThat(buffer.stats().backlog()).isZero();
        assertThat(buffer.stats().oldestUnackedSequence()).isEqualTo(-1);
        assertThat(buffer.committedPosition()).isEqualTo(3);
    }

    @Test
    @DisplayName("重放隔离：从指定位点重放不影响正在进行的消费位点")
    void replayDoesNotAffectLiveConsumptionPositions() {
        buffer.append(KEY_A, "a1");
        buffer.append(KEY_A, "a2");
        buffer.append(KEY_B, "b1");
        buffer.append(KEY_A, "a3");

        // 实时消费推进一位
        buffer.ack(buffer.poll(KEY_A).orElseThrow().sequence());
        long emitBefore = buffer.stats().emitCount();

        // 从位点 2 重放：按全局序号产出仍保留的事件
        ReplayCursor cursor = buffer.replayFrom(2L);
        List<Long> replayed = new ArrayList<>();
        cursor.forEachRemaining(e -> replayed.add(e.sequence()));
        assertThat(replayed).containsExactly(2L, 3L, 4L);

        // 重放不改变统计与实时位点
        assertThat(buffer.stats().emitCount()).isEqualTo(emitBefore);
        assertThat(buffer.poll(KEY_A)).hasValueSatisfying(e -> assertThat(e.sequence()).isEqualTo(2L));
        assertThat(buffer.poll(KEY_B)).hasValueSatisfying(e -> assertThat(e.sequence()).isEqualTo(3L));
    }

    @Test
    @DisplayName("统计：写入数、产出数、隔离数、积压量与最老未确认序号")
    void statsReflectBufferLifecycle() {
        ChangeEventBuffer buffer = new ChangeEventBuffer(1);
        buffer.append(KEY_A, "a1");
        buffer.append(KEY_B, "b1");
        buffer.append(KEY_B, "b2");

        BufferStats initial = buffer.stats();
        assertThat(initial.writeCount()).isEqualTo(3);
        assertThat(initial.emitCount()).isZero();
        assertThat(initial.quarantinedCount()).isZero();
        assertThat(initial.backlog()).isEqualTo(3);
        assertThat(initial.oldestUnackedSequence()).isEqualTo(1);

        buffer.fail(buffer.poll(KEY_A).orElseThrow().sequence()); // maxRetries=1，直接隔离
        buffer.ack(buffer.poll(KEY_B).orElseThrow().sequence());

        BufferStats stats = buffer.stats();
        assertThat(stats.writeCount()).isEqualTo(3);
        assertThat(stats.emitCount()).isEqualTo(2);
        assertThat(stats.quarantinedCount()).isEqualTo(1);
        assertThat(stats.backlog()).isEqualTo(1);
        assertThat(stats.oldestUnackedSequence()).isEqualTo(3);
    }

    @Test
    @DisplayName("非法操作：确认/失败非在途序号、重放越界位点均抛异常")
    void invalidOperationsAreRejected() {
        buffer.append(KEY_A, "a1");

        assertThatThrownBy(() -> buffer.ack(1L)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> buffer.fail(99L)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> buffer.replayFrom(0L)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> buffer.replayFrom(2L)).isInstanceOf(IllegalArgumentException.class);
    }
}
