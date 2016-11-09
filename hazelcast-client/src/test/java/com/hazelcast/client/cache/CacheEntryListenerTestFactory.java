package com.hazelcast.client.cache;

import javax.cache.configuration.Factory;
import java.util.concurrent.CountDownLatch;

/**
 * Created by ihsan on 31/10/16.
 */
public class CacheEntryListenerTestFactory<K, V> implements Factory<ClientCacheEntryExpiredLatchCountdownListener<K, V>> {
    @Override
    public ClientCacheEntryExpiredLatchCountdownListener<K, V> create() {
        return new ClientCacheEntryExpiredLatchCountdownListener<K, V>();
    }
}

