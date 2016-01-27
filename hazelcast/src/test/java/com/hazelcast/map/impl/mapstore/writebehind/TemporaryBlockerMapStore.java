package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.core.MapStoreAdapter;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;

public class TemporaryBlockerMapStore extends MapStoreAdapter<String, String> {

    private final int blockStoreOperationSeconds;
    private final AtomicInteger storeOperationCount = new AtomicInteger(0);

    public TemporaryBlockerMapStore(int blockStoreOperationSeconds) {
        this.blockStoreOperationSeconds = blockStoreOperationSeconds;
    }

    @Override
    public void store(String key, String value) {
        storeOperationCount.incrementAndGet();

        sleepSeconds(blockStoreOperationSeconds);
    }

    public int getStoreOperationCount() {
        return storeOperationCount.get();
    }
}