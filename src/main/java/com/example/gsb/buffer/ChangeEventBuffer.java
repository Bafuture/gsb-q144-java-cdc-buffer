package com.example.gsb.buffer;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * 变更事件缓冲组件。
 *
 * <p>语义：
 * <ul>
 *   <li>写入时分配全局连续序号（1 起，不跳号）；同一聚合键严格按序号顺序产出。</li>
 *   <li>按聚合键分区：每个分区最多一个在途（已投递未确认）事件，不同分区可并行消费。</li>
 *   <li>下游失败可调用 {@link #fail} 触发重试；重试次数达到上限后事件进入隔离区（死信），
 *       该聚合键的后续事件与其它分区均不受影响。</li>
 *   <li>{@link #ack} 确认某序号后，该事件立即从缓冲中清理；{@link #committedPosition()}
 *       给出连续已确认位点。</li>
 *   <li>{@link #replayFrom} 基于快照重放，不影响实时消费位点。</li>
 * </ul>
 *
 * <p>所有公共方法均为同步方法，可安全用于多线程生产/消费。
 */
public final class ChangeEventBuffer {

    private final int maxRetries;
    /** 已写入但尚未确认（含在途）的事件，按全局序号升序。 */
    private final TreeMap<Long, ChangeEvent> retained = new TreeMap<>();
    private final Map<String, Partition> partitions = new HashMap<>();
    private final Map<Long, String> inFlightOwners = new HashMap<>();
    private final List<ChangeEvent> deadLetters = new ArrayList<>();

    private long lastAssignedSequence;
    private long writeCount;
    private long emitCount;
    private long quarantinedCount;

    /**
     * @param maxRetries 单事件最大投递次数（含首次），达到后进入隔离区；必须 &gt;= 1
     */
    public ChangeEventBuffer(int maxRetries) {
        if (maxRetries < 1) {
            throw new IllegalArgumentException("maxRetries must be >= 1");
        }
        this.maxRetries = maxRetries;
    }

    /** 写入一条事件，返回携带全局序号的不可变事件对象。 */
    public synchronized ChangeEvent append(String aggregateKey, String payload) {
        Objects.requireNonNull(aggregateKey, "aggregateKey");
        Objects.requireNonNull(payload, "payload");
        long sequence = ++lastAssignedSequence;
        ChangeEvent event = new ChangeEvent(sequence, aggregateKey, payload, Instant.now());
        retained.put(sequence, event);
        partitionOf(aggregateKey).pending.addLast(sequence);
        writeCount++;
        return event;
    }

    /**
     * 取出指定聚合键的下一个待投递事件并置为在途。
     * 该分区已有在途事件或无待投递事件时返回 {@link Optional#empty()}。
     */
    public synchronized Optional<ChangeEvent> poll(String aggregateKey) {
        Objects.requireNonNull(aggregateKey, "aggregateKey");
        Partition partition = partitions.get(aggregateKey);
        if (partition == null || partition.inFlight != null || partition.pending.isEmpty()) {
            return Optional.empty();
        }
        long sequence = partition.pending.pollFirst();
        partition.inFlight = sequence;
        inFlightOwners.put(sequence, aggregateKey);
        emitCount++;
        return Optional.of(retained.get(sequence));
    }

    /**
     * 确认在途事件处理成功：推进分区位点，并将该事件从缓冲中清理。
     *
     * @throws IllegalStateException 该序号不是任何分区的在途事件
     */
    public synchronized void ack(long sequence) {
        Partition partition = inFlightPartitionOf(sequence);
        partition.inFlight = null;
        inFlightOwners.remove(sequence);
        retained.remove(sequence);
    }

    /**
     * 上报在途事件处理失败：未达重试上限则回到队首等待重投，达到上限则进入隔离区。
     * 进入隔离区仅跳过该事件本身，不阻塞其后的同键事件与其它分区。
     *
     * @throws IllegalStateException 该序号不是任何分区的在途事件
     */
    public synchronized void fail(long sequence) {
        Partition partition = inFlightPartitionOf(sequence);
        partition.attempts++;
        partition.inFlight = null;
        inFlightOwners.remove(sequence);
        if (partition.attempts >= maxRetries) {
            partition.attempts = 0;
            deadLetters.add(retained.remove(sequence));
            quarantinedCount++;
        } else {
            partition.pending.addFirst(sequence);
        }
    }

    /**
     * 从指定位点重放：返回基于当前快照的游标，按全局序号升序产出所有
     * 序号 &gt;= fromSequence 且仍保留在缓冲中的事件。重放不改变任何消费位点。
     *
     * @throws IllegalArgumentException 位点越界或对应事件已被清理/隔离
     */
    public synchronized ReplayCursor replayFrom(long fromSequence) {
        if (fromSequence < 1 || fromSequence > lastAssignedSequence) {
            throw new IllegalArgumentException(
                    "fromSequence " + fromSequence + " out of range [1, " + lastAssignedSequence + "]");
        }
        if (!retained.containsKey(fromSequence)) {
            throw new IllegalArgumentException(
                    "sequence " + fromSequence + " has been cleaned up or quarantined");
        }
        return new ReplayCursor(new ArrayList<>(retained.tailMap(fromSequence).values()));
    }

    /** 连续已确认位点：所有序号 &lt;= 该值的事件均已被确认清理。 */
    public synchronized long committedPosition() {
        return retained.isEmpty() ? lastAssignedSequence : retained.firstKey() - 1;
    }

    /** 隔离区（死信）中的事件，按进入顺序排列。 */
    public synchronized List<ChangeEvent> deadLetters() {
        return Collections.unmodifiableList(new ArrayList<>(deadLetters));
    }

    public synchronized BufferStats stats() {
        long oldest = retained.isEmpty() ? -1 : retained.firstKey();
        return new BufferStats(writeCount, emitCount, quarantinedCount, retained.size(), oldest);
    }

    private Partition partitionOf(String aggregateKey) {
        return partitions.computeIfAbsent(aggregateKey, key -> new Partition());
    }

    private Partition inFlightPartitionOf(long sequence) {
        String owner = inFlightOwners.get(sequence);
        if (owner == null) {
            throw new IllegalStateException("sequence " + sequence + " is not in flight");
        }
        return partitions.get(owner);
    }

    private static final class Partition {
        private final Deque<Long> pending = new ArrayDeque<>();
        private Long inFlight;
        private int attempts;
    }
}
