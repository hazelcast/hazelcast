package com.hazelcast.spring;

import com.hazelcast.cache.impl.event.CachePartitionLostEvent;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;


public class DummyCachePartitionLostListenerImpl implements CachePartitionLostListener {
    @Override
    public void partitionLost(CachePartitionLostEvent event) {

    }
}
