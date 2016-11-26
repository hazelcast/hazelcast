package com.hazelcast.client.cache;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by ihsan on 31/10/16.
 */
public class ClientCacheEntryExpiredLatchCountdownListener<K, V> implements javax.cache.event.CacheEntryExpiredListener<K, V> {
    private transient final static CountDownLatch expiredLatch = new CountDownLatch(2);

    @Override
    public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents) throws
            CacheEntryListenerException {
        for (CacheEntryEvent<? extends K, ? extends V> event : cacheEntryEvents) {
            System.err.println("ClientCacheEntryExpiredLatchCountdownListener expired: " + event.getKey());
        }
        expiredLatch.countDown();
    }

    public static CountDownLatch getExpiredLatch() {
        return expiredLatch;
    }
}


