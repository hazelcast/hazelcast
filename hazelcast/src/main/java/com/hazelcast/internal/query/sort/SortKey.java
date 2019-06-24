package com.hazelcast.internal.query.sort;

import java.util.List;

public class SortKey {
    private final List<Object> key;
    private final long idx;

    public SortKey(List<Object> key, long idx) {
        this.key = key;
        this.idx = idx;
    }

    public List<Object> getKey() {
        return key;
    }

    public long getIdx() {
        return idx;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(idx);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SortKey) {
            SortKey other = (SortKey)obj;

            return other.idx == ((SortKey)obj).idx;
        }
        else
            return false;
    }
}
