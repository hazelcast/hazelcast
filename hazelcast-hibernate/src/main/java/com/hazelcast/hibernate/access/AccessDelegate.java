package com.hazelcast.hibernate.access;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.access.SoftLock;

import com.hazelcast.core.IMap;
import com.hazelcast.hibernate.region.HazelcastRegion;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
public interface AccessDelegate<T extends HazelcastRegion> {
    T getHazelcastRegion();

    IMap getCache();

    boolean afterInsert(Object key, Object value, Object version) throws CacheException;

    boolean afterUpdate(Object key, Object value, Object currentVersion, Object previousVersion, SoftLock lock)
            throws CacheException;

    void evict(Object key) throws CacheException;

    void evictAll() throws CacheException;

    Object get(Object key, long txTimestamp) throws CacheException;

    boolean insert(Object key, Object value, Object version) throws CacheException;

    SoftLock lockItem(Object key, Object version) throws CacheException;

    SoftLock lockRegion() throws CacheException;

    boolean putFromLoad(Object key, Object value, long txTimestamp, Object version) throws CacheException;

    boolean putFromLoad(Object key, Object value, long txTimestamp, Object version, boolean minimalPutOverride)
            throws CacheException;

    void remove(Object key) throws CacheException;

    void removeAll() throws CacheException;

    void unlockItem(Object key, SoftLock lock) throws CacheException;

    void unlockRegion(SoftLock lock) throws CacheException;

    boolean update(Object key, Object value, Object currentVersion, Object previousVersion) throws CacheException;
}
