package com.example.gsb.buffer;

import java.util.List;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 重放游标：基于创建时刻的不可变快照按全局序号升序产出事件，
 * 与实时消费位点完全隔离，互不影响。
 */
public final class ReplayCursor implements Iterator<ChangeEvent> {

    private final List<ChangeEvent> snapshot;
    private int index;

    ReplayCursor(List<ChangeEvent> snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public boolean hasNext() {
        return index < snapshot.size();
    }

    @Override
    public ChangeEvent next() {
        if (!hasNext()) {
            throw new NoSuchElementException("replay cursor exhausted");
        }
        return snapshot.get(index++);
    }

    public int remaining() {
        return snapshot.size() - index;
    }
}
