package com.example.gsb.buffer;

import java.time.Instant;
import java.util.Objects;

/**
 * 一条变更事件。全局序号由 {@link ChangeEventBuffer} 在写入时分配，从 1 开始连续递增、不跳号。
 */
public final class ChangeEvent {

    private final long sequence;
    private final String aggregateKey;
    private final String payload;
    private final Instant occurredAt;

    public ChangeEvent(long sequence, String aggregateKey, String payload, Instant occurredAt) {
        if (sequence < 1) {
            throw new IllegalArgumentException("sequence must be >= 1");
        }
        this.sequence = sequence;
        this.aggregateKey = Objects.requireNonNull(aggregateKey, "aggregateKey");
        this.payload = Objects.requireNonNull(payload, "payload");
        this.occurredAt = Objects.requireNonNull(occurredAt, "occurredAt");
    }

    public long sequence() {
        return sequence;
    }

    public String aggregateKey() {
        return aggregateKey;
    }

    public String payload() {
        return payload;
    }

    public Instant occurredAt() {
        return occurredAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ChangeEvent other)) {
            return false;
        }
        return sequence == other.sequence
                && aggregateKey.equals(other.aggregateKey)
                && payload.equals(other.payload)
                && occurredAt.equals(other.occurredAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequence, aggregateKey, payload, occurredAt);
    }

    @Override
    public String toString() {
        return "ChangeEvent{sequence=" + sequence
                + ", aggregateKey='" + aggregateKey + '\''
                + ", payload='" + payload + '\''
                + ", occurredAt=" + occurredAt + '}';
    }
}
