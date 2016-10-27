package com.hazelcast.spring;

import com.hazelcast.core.RingbufferStore;

public class DummyRingbufferStore implements RingbufferStore {
    @Override
    public void store(long sequence, Object data) {
    }

    @Override
    public void storeAll(long firstItemSequence, Object[] items) {

    }

    @Override
    public Object load(long sequence) {
        return null;
    }

    @Override
    public long getLargestSequence() {
        return 0;
    }
}
