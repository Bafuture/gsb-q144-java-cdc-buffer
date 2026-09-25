package com.example.gsb.buffer;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * 变更事件缓冲组件。
 *
 * <p>语义：
 * <ul>
 *   <li>写入时分配全局序号，从 1 开始连续递增、不跳号；</li>
 *   <li>按聚合键分区：同一聚合键严格按序号顺序产出，且同一时刻至多一个在途事件；
 *       不同聚合键互不影响，可并行消费（轮询调度保证公平）；</li>
 *   <li>下游处理失败调用 {@link #fail(long)} 重试，重试次数超过 {@code maxRetries}
 *       后事件进入隔离区（死信），该聚合键的后续事件与其它分区均不被阻塞；</li>
 *   <li>{@link #commit(long)} 提交位点后，序号不超过位点的事件从缓冲中清理；</li>
 *   <li>{@link #replay(long)} 从指定位点重放仍在缓冲中的事件，不影响消费位点。</li>
 * </ul>
 *
 * <p>线程安全：所有公开方法均为同步方法。
 */
public final class ChangeEventBuffer<T> {

    /** 默认最大重试次数。 */
    public static final int DEFAULT_MAX_RETRIES = 3;

    private final int maxRetries;

    /** 仍在保留期内的事件（含待产出、在途、已确认未提交、已隔离未提交），按序号排序。 */
    private final NavigableMap<Long, ChangeEvent<T>> store = new TreeMap<>();
    /** 每个聚合键的待产出序号队列。 */
    private final Map<String, ArrayDeque<Long>> pendingByKey = new HashMap<>();
    /** 每个聚合键当前在途（已产出未确认）的序号。 */
    private final Map<String, Long> inFlightByKey = new HashMap<>();
    /** 每个序号的已产出次数。 */
    private final Map<Long, Integer> attemptsBySeq = new HashMap<>();
    /** 隔离区（死信），按进入顺序。 */
    private final Map<Long, ChangeEvent<T>> deadLetters = new LinkedHashMap<>();
    /** 可立即产出的聚合键轮询队列（配合 readySet 去重）。 */
    private final ArrayDeque<String> readyKeys = new ArrayDeque<>();
    private final Set<String> readySet = new HashSet<>();

    private long lastSequence;
    private long committed;
    private long written;
    private long delivered;
    private long deadLettered;

    public ChangeEventBuffer() {
        this(DEFAULT_MAX_RETRIES);
    }

    public ChangeEventBuffer(int maxRetries) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries 不能为负数: " + maxRetries);
        }
        this.maxRetries = maxRetries;
    }

    /**
     * 写入一条事件，返回分配的全局序号。
     */
    public synchronized long append(String aggregateKey, T payload) {
        Objects.requireNonNull(aggregateKey, "aggregateKey");
        lastSequence++;
        written++;
        long sequence = lastSequence;
        store.put(sequence, new ChangeEvent<>(sequence, aggregateKey, payload, Instant.now(), 0));
        pendingByKey.computeIfAbsent(aggregateKey, k -> new ArrayDeque<>()).add(sequence);
        attemptsBySeq.put(sequence, 0);
        markReady(aggregateKey);
        return sequence;
    }

    /**
     * 产出下一条可消费的事件；没有可产出的事件时返回 {@code null}。
     *
     * <p>同一聚合键在上一条事件被 {@link #ack(long)} 或 {@link #fail(long)} 处理前
     * 不会产出下一条；不同聚合键之间轮询调度、互不影响。
     */
    public synchronized ChangeEvent<T> poll() {
        while (!readyKeys.isEmpty()) {
            String key = readyKeys.poll();
            readySet.remove(key);
            if (inFlightByKey.containsKey(key)) {
                continue;
            }
            ArrayDeque<Long> queue = pendingByKey.get(key);
            if (queue == null || queue.isEmpty()) {
                continue;
            }
            long sequence = queue.poll();
            inFlightByKey.put(key, sequence);
            int attempts = attemptsBySeq.merge(sequence, 1, Integer::sum);
            return store.get(sequence).withAttempts(attempts);
        }
        return null;
    }

    /**
     * 确认一条在途事件处理成功。
     */
    public synchronized void ack(long sequence) {
        String key = requireInFlight(sequence);
        inFlightByKey.remove(key);
        attemptsBySeq.remove(sequence);
        delivered++;
        markReady(key);
    }

    /**
     * 报告一条在途事件处理失败。未超过最大重试次数时重新排队（仍先于该聚合键
     * 后续事件产出）；超过后进入隔离区，该聚合键及其它分区继续推进。
     */
    public synchronized void fail(long sequence) {
        String key = requireInFlight(sequence);
        inFlightByKey.remove(key);
        int attempts = attemptsBySeq.get(sequence);
        if (attempts > maxRetries) {
            attemptsBySeq.remove(sequence);
            deadLetters.put(sequence, store.get(sequence));
            deadLettered++;
        } else {
            ArrayDeque<Long> queue = pendingByKey.get(key);
            queue.addFirst(sequence);
        }
        markReady(key);
    }

    /**
     * 提交消费位点：序号不超过 {@code sequence} 的事件全部从缓冲中清理。
     *
     * <p>要求序号不超过 {@code sequence} 的事件均已被确认或隔离，否则抛出
     * {@link IllegalStateException}。重复或回退提交为幂等空操作。
     */
    public synchronized void commit(long sequence) {
        if (sequence <= committed) {
            return;
        }
        if (sequence > lastSequence) {
            throw new IllegalArgumentException(
                    "位点 " + sequence + " 超出已写入的最大序号 " + lastSequence);
        }
        long smallestUnresolved = smallestUnresolved();
        if (smallestUnresolved != -1 && sequence >= smallestUnresolved) {
            throw new IllegalStateException(
                    "序号 " + smallestUnresolved + " 及之前仍存在未确认/未隔离的事件，不能提交位点 " + sequence);
        }
        store.headMap(sequence, true).clear();
        committed = sequence;
    }

    /**
     * 从指定位点（含）重放仍在缓冲中的事件，返回按序号排序的快照。
     *
     * <p>重放是纯只读操作，不改变任何消费位点与分区状态。
     *
     * @throws IllegalArgumentException 位点已被 {@link #commit(long)} 清理
     */
    public synchronized List<ChangeEvent<T>> replay(long fromSequence) {
        if (fromSequence <= committed) {
            throw new IllegalArgumentException(
                    "位点 " + fromSequence + " 及之前的事件已随提交被清理，无法重放");
        }
        if (fromSequence > lastSequence) {
            return List.of();
        }
        return List.copyOf(store.tailMap(fromSequence, true).values());
    }

    /**
     * 隔离区（死信）中的事件，按进入顺序。
     */
    public synchronized List<ChangeEvent<T>> deadLetters() {
        return List.copyOf(deadLetters.values());
    }

    /**
     * 当前统计快照。
     */
    public synchronized BufferStats stats() {
        long backlog = written - delivered - deadLettered;
        long oldestUnconfirmed = committed < lastSequence ? committed + 1 : -1;
        return new BufferStats(written, delivered, deadLettered, backlog, oldestUnconfirmed);
    }

    /** 已分配的最大序号（尚未写入时为 0）。 */
    public synchronized long lastSequence() {
        return lastSequence;
    }

    /** 当前已提交位点。 */
    public synchronized long committedPosition() {
        return committed;
    }

    public synchronized int maxRetries() {
        return maxRetries;
    }

    private void markReady(String key) {
        ArrayDeque<Long> queue = pendingByKey.get(key);
        if (queue == null || queue.isEmpty() || inFlightByKey.containsKey(key)) {
            return;
        }
        if (readySet.add(key)) {
            readyKeys.add(key);
        }
    }

    private String requireInFlight(long sequence) {
        ChangeEvent<T> event = store.get(sequence);
        if (event == null) {
            throw new IllegalArgumentException("序号 " + sequence + " 不在缓冲中（不存在或已清理）");
        }
        String key = event.aggregateKey();
        Long inFlight = inFlightByKey.get(key);
        if (inFlight == null || inFlight != sequence) {
            throw new IllegalStateException("序号 " + sequence + " 当前不在途，不能确认/失败");
        }
        return key;
    }

    private long smallestUnresolved() {
        long smallest = -1;
        List<Long> candidates = new ArrayList<>();
        for (ArrayDeque<Long> queue : pendingByKey.values()) {
            if (!queue.isEmpty()) {
                candidates.add(queue.peek());
            }
        }
        candidates.addAll(inFlightByKey.values());
        for (long seq : candidates) {
            if (smallest == -1 || seq < smallest) {
                smallest = seq;
            }
        }
        return smallest;
    }
}
