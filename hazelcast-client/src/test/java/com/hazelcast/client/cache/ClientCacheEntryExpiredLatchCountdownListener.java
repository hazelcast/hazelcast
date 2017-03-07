package com.hazelcast.client.cache;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import java.util.concurrent.CountDownLatch;

public class ClientCacheEntryExpiredLatchCountdownListener<K, V> implements javax.cache.event.CacheEntryExpiredListener<K, V> {

    private transient static final CountDownLatch EXPIRED_LATCH = new CountDownLatch(2);

    @Override
    public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
            throws CacheEntryListenerException {
        for (CacheEntryEvent<? extends K, ? extends V> event : cacheEntryEvents) {
            System.err.println("ClientCacheEntryExpiredLatchCountdownListener expired: " + event.getKey());
        }
        EXPIRED_LATCH.countDown();
    }

    public static CountDownLatch getExpiredLatch() {
        return EXPIRED_LATCH;
    }
}
