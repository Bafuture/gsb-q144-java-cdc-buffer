package com.example.gsb.buffer;

import java.time.Instant;
import java.util.Objects;

/**
 * 一条变更事件。全局序号由 {@link ChangeEventBuffer} 在写入时分配，从 1 开始连续递增、不跳号。
 */
public final class ChangeEvent<T> {

    private final long sequence;
    private final String aggregateKey;
    private final T payload;
    private final Instant createdAt;
    private final int attempts;

    ChangeEvent(long sequence, String aggregateKey, T payload, Instant createdAt, int attempts) {
        this.sequence = sequence;
        this.aggregateKey = Objects.requireNonNull(aggregateKey, "aggregateKey");
        this.payload = payload;
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
        this.attempts = attempts;
    }

    /** 全局序号，从 1 开始连续递增。 */
    public long sequence() {
        return sequence;
    }

    /** 聚合键，同一聚合键的事件严格按序号顺序产出。 */
    public String aggregateKey() {
        return aggregateKey;
    }

    public T payload() {
        return payload;
    }

    public Instant createdAt() {
        return createdAt;
    }

    /** 已被产出（投递）的次数，重试时递增。 */
    public int attempts() {
        return attempts;
    }

    ChangeEvent<T> withAttempts(int newAttempts) {
        return new ChangeEvent<>(sequence, aggregateKey, payload, createdAt, newAttempts);
    }

    @Override
    public String toString() {
        return "ChangeEvent{sequence=" + sequence
                + ", aggregateKey='" + aggregateKey + '\''
                + ", payload=" + payload
                + ", attempts=" + attempts + '}';
    }
}
