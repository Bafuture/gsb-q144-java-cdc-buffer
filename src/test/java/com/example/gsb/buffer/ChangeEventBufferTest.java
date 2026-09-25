package com.example.gsb.buffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ChangeEventBufferTest {

    private final ChangeEventBuffer<String> buffer = new ChangeEventBuffer<>(2);

    private static List<Long> sequencesOf(List<ChangeEvent<String>> events) {
        return events.stream().map(ChangeEvent::sequence).toList();
    }

    @Nested
    @DisplayName("顺序保证")
    class Ordering {

        @Test
        @DisplayName("全局序号从 1 开始连续递增、不跳号，同键严格按序号产出")
        void gaplessSequencesAndInOrderDelivery() {
            for (int i = 1; i <= 10; i++) {
                assertThat(buffer.append("order", "payload-" + i)).isEqualTo(i);
            }
            for (int expected = 1; expected <= 10; expected++) {
                ChangeEvent<String> event = buffer.poll();
                assertThat(event).isNotNull();
                assertThat(event.sequence()).isEqualTo(expected);
                assertThat(event.payload()).isEqualTo("payload-" + expected);
                buffer.ack(event.sequence());
            }
            assertThat(buffer.poll()).isNull();
        }

        @Test
        @DisplayName("交错写入多个聚合键时，每个键内部仍严格按序号顺序产出")
        void perKeyOrderPreservedWhenInterleaved() {
            buffer.append("a", "a1"); // 1
            buffer.append("b", "b1"); // 2
            buffer.append("a", "a2"); // 3
            buffer.append("b", "b2"); // 4
            buffer.append("a", "a3"); // 5

            List<Long> keyA = new ArrayList<>();
            List<Long> keyB = new ArrayList<>();
            ChangeEvent<String> event;
            while ((event = buffer.poll()) != null) {
                (event.aggregateKey().equals("a") ? keyA : keyB).add(event.sequence());
                buffer.ack(event.sequence());
            }
            assertThat(keyA).containsExactly(1L, 3L, 5L);
            assertThat(keyB).containsExactly(2L, 4L);
        }
    }

    @Nested
    @DisplayName("分区并行")
    class Partitioning {

        @Test
        @DisplayName("同一聚合键至多一个在途事件，其它键不受影响可并行产出")
        void oneInFlightPerKeyOthersProceed() {
            buffer.append("a", "a1"); // 1
            buffer.append("a", "a2"); // 2
            buffer.append("b", "b1"); // 3
            buffer.append("b", "b2"); // 4

            assertThat(buffer.poll().sequence()).isEqualTo(1L); // a 在途
            assertThat(buffer.poll().sequence()).isEqualTo(3L); // b 可并行
            assertThat(buffer.poll()).isNull();                 // 两键各有在途，无可产出

            buffer.ack(1);
            assertThat(buffer.poll().sequence()).isEqualTo(2L); // a 推进
            buffer.ack(3);
            assertThat(buffer.poll().sequence()).isEqualTo(4L); // b 推进
        }
    }

    @Nested
    @DisplayName("失败重试与隔离")
    class FailureIsolation {

        @Test
        @DisplayName("重试期间同键后续事件等待，超过重试上限进入死信后该键继续推进")
        void retryThenDeadLetterThenKeyProceeds() {
            buffer.append("a", "poison"); // 1
            buffer.append("a", "a2");     // 2

            ChangeEvent<String> first = buffer.poll();
            assertThat(first.sequence()).isEqualTo(1L);
            assertThat(first.attempts()).isEqualTo(1);
            buffer.fail(1);

            ChangeEvent<String> retry = buffer.poll();
            assertThat(retry.sequence()).isEqualTo(1L); // 重试仍先于同键后续事件
            assertThat(retry.attempts()).isEqualTo(2);
            buffer.fail(1);

            ChangeEvent<String> lastAttempt = buffer.poll();
            assertThat(lastAttempt.sequence()).isEqualTo(1L);
            assertThat(lastAttempt.attempts()).isEqualTo(3);
            buffer.fail(1); // 第 3 次失败，超过 maxRetries=2，进入死信

            assertThat(sequencesOf(buffer.deadLetters())).containsExactly(1L);

            ChangeEvent<String> next = buffer.poll();
            assertThat(next.sequence()).isEqualTo(2L); // 死信不阻塞同键后续事件
            buffer.ack(2);
        }

        @Test
        @DisplayName("一个分区的失败与死信不阻塞其它分区")
        void deadLetterDoesNotBlockOtherPartitions() {
            buffer.append("a", "poison"); // 1
            buffer.append("b", "b1");     // 2
            buffer.append("a", "a2");     // 3

            buffer.fail(buffer.poll().sequence()); // a: 1 第 1 次失败
            assertThat(buffer.poll().sequence()).isEqualTo(2L); // b 正常产出
            buffer.ack(2);

            buffer.fail(buffer.poll().sequence()); // a: 1 第 2 次失败
            buffer.fail(buffer.poll().sequence()); // a: 1 第 3 次失败 → 死信

            assertThat(sequencesOf(buffer.deadLetters())).containsExactly(1L);
            assertThat(buffer.poll().sequence()).isEqualTo(3L); // a 继续推进
            buffer.ack(3);
            assertThat(buffer.poll()).isNull();

            BufferStats stats = buffer.stats();
            assertThat(stats.written()).isEqualTo(3);
            assertThat(stats.delivered()).isEqualTo(2);
            assertThat(stats.deadLettered()).isEqualTo(1);
            assertThat(stats.backlog()).isZero();
        }
    }

    @Nested
    @DisplayName("位点提交")
    class Commit {

        @Test
        @DisplayName("提交位点后已确认事件被清理，最老未确认序号随之前进")
        void commitCleansConfirmedEvents() {
            for (int i = 0; i < 5; i++) {
                buffer.append("a", "p" + i);
            }
            for (int i = 0; i < 5; i++) {
                buffer.ack(buffer.poll().sequence());
            }

            buffer.commit(3);

            assertThat(buffer.committedPosition()).isEqualTo(3);
            assertThat(buffer.stats().oldestUnconfirmedSequence()).isEqualTo(4);
            assertThatThrownBy(() -> buffer.replay(3))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(sequencesOf(buffer.replay(4))).containsExactly(4L, 5L);

            buffer.commit(5);
            assertThat(buffer.stats().oldestUnconfirmedSequence()).isEqualTo(-1);
        }

        @Test
        @DisplayName("存在未确认事件时不能提交越过它的位点")
        void commitBeyondUnresolvedEventRejected() {
            buffer.append("a", "p1");
            buffer.append("a", "p2");
            buffer.ack(buffer.poll().sequence()); // 仅确认 1

            assertThatThrownBy(() -> buffer.commit(2))
                    .isInstanceOf(IllegalStateException.class);
            buffer.commit(1); // 不越过未确认事件，允许
            assertThat(buffer.committedPosition()).isEqualTo(1);
        }

        @Test
        @DisplayName("提交超出已写入序号的位点被拒绝，重复提交幂等")
        void commitValidation() {
            buffer.append("a", "p1");
            assertThatThrownBy(() -> buffer.commit(2))
                    .isInstanceOf(IllegalArgumentException.class);
            buffer.ack(buffer.poll().sequence());
            buffer.commit(1);
            buffer.commit(1); // 幂等空操作
            assertThat(buffer.committedPosition()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("重放")
    class Replay {

        @Test
        @DisplayName("重放返回指定位点起的快照，且不影响正在进行的消费位点")
        void replayDoesNotAffectConsumptionPosition() {
            buffer.append("a", "p1"); // 1
            buffer.append("b", "p2"); // 2
            buffer.append("a", "p3"); // 3
            buffer.append("b", "p4"); // 4

            buffer.ack(buffer.poll().sequence()); // 消费 1
            buffer.ack(buffer.poll().sequence()); // 消费 2

            List<ChangeEvent<String>> replayed = buffer.replay(1);
            assertThat(sequencesOf(replayed)).containsExactly(1L, 2L, 3L, 4L);
            assertThat(sequencesOf(buffer.replay(3))).containsExactly(3L, 4L);

            // 重放之后消费位点不变：下一条仍是 3
            assertThat(buffer.poll().sequence()).isEqualTo(3L);
            buffer.ack(3);
            assertThat(buffer.poll().sequence()).isEqualTo(4L);
        }

        @Test
        @DisplayName("重放结果为不可变快照，不随后续写入变化")
        void replayReturnsImmutableSnapshot() {
            buffer.append("a", "p1");
            List<ChangeEvent<String>> snapshot = buffer.replay(1);
            buffer.append("a", "p2");

            assertThat(sequencesOf(snapshot)).containsExactly(1L);
            assertThatThrownBy(() -> snapshot.add(snapshot.get(0)))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    @DisplayName("统计")
    class Statistics {

        @Test
        @DisplayName("写入数、产出数、隔离数、积压量与最老未确认序号均准确")
        void statsAreAccurate() {
            assertThat(buffer.stats()).isEqualTo(new BufferStats(0, 0, 0, 0, -1));

            buffer.append("a", "p1"); // 1
            buffer.append("b", "p2"); // 2
            buffer.append("b", "p3"); // 3
            assertThat(buffer.stats()).isEqualTo(new BufferStats(3, 0, 0, 3, 1));

            buffer.ack(buffer.poll().sequence()); // 确认 1
            assertThat(buffer.stats()).isEqualTo(new BufferStats(3, 1, 0, 2, 1));

            buffer.commit(1);
            assertThat(buffer.stats()).isEqualTo(new BufferStats(3, 1, 0, 2, 2));

            // b 的事件 2 三次失败进入死信
            buffer.fail(buffer.poll().sequence());
            buffer.fail(buffer.poll().sequence());
            buffer.fail(buffer.poll().sequence());
            assertThat(buffer.stats()).isEqualTo(new BufferStats(3, 1, 1, 1, 2));

            buffer.ack(buffer.poll().sequence()); // 确认 3
            buffer.commit(3);
            assertThat(buffer.stats()).isEqualTo(new BufferStats(3, 2, 1, 0, -1));
        }
    }

    @Nested
    @DisplayName("边界与校验")
    class Validation {

        @Test
        @DisplayName("空缓冲产出返回 null")
        void pollOnEmptyReturnsNull() {
            assertThat(buffer.poll()).isNull();
        }

        @Test
        @DisplayName("确认/失败不在途的序号被拒绝")
        void ackOrFailNonInFlightRejected() {
            buffer.append("a", "p1");
            assertThatThrownBy(() -> buffer.ack(1)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> buffer.ack(99)).isInstanceOf(IllegalArgumentException.class);
            buffer.poll();
            buffer.ack(1);
            assertThatThrownBy(() -> buffer.fail(1)).isInstanceOf(IllegalStateException.class);
        }

        @Test
        @DisplayName("非法 maxRetries 被拒绝")
        void negativeMaxRetriesRejected() {
            assertThatThrownBy(() -> new ChangeEventBuffer<String>(-1))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
