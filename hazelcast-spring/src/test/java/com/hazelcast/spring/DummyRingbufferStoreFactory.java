package com.hazelcast.spring;

import com.hazelcast.core.RingbufferStore;
import com.hazelcast.core.RingbufferStoreFactory;

import java.util.Properties;

public class DummyRingbufferStoreFactory implements RingbufferStoreFactory {
    @Override
    public RingbufferStore newRingbufferStore(String name, Properties properties) {
        return new DummyRingbufferStore();
    }
}
