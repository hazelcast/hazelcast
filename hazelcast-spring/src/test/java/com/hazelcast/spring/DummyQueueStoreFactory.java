package com.hazelcast.spring;

import com.hazelcast.core.QueueStore;
import com.hazelcast.core.QueueStoreFactory;

import java.util.Properties;

public class DummyQueueStoreFactory implements QueueStoreFactory {

    @Override
    public QueueStore newQueueStore(String name, Properties properties) {
        return new DummyQueueStore();
    }
}
