package com.hazelcast.core;

import java.util.Properties;

/**
 * @ali 9/16/13
 */
public interface QueueStoreFactory<T> {

    QueueStore<T> newQueueStore(String name, Properties properties);
}
