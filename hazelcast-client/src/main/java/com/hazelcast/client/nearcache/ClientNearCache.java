package com.hazelcast.client.nearcache;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.monitor.NearCacheStats;

/**
 * ClientNearCache
 *
 * @param <K> key type
 */
public interface ClientNearCache<K, V> {

    /**
     * Eviction percentage
     */
    int EVICTION_PERCENTAGE = 20;

    /**
     * TTL Clean up interval
     */
    int TTL_CLEANUP_INTERVAL_MILLS = 5000;

    /**
     * NULL Object
     */
    Object NULL_OBJECT = new Object();

    V get(K key);

    void put(K key, V object);

    void remove(K key);

    void invalidate(K key);

    void clear();

    void destroy();

    boolean isInvalidateOnChange();

    InMemoryFormat getInMemoryFormat();

    void setId(String id);

    String getId();

    NearCacheStats getNearCacheStats();

}
