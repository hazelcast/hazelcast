package com.hazelcast.hibernate;

import org.hibernate.cache.access.SoftLock;

import java.util.Map;

/**
 * @mdogan 11/9/12
 */
public interface RegionCache {

    Object get(final Object key);

    boolean put(final Object key, final Object value,
                final Object currentVersion, final Object previousVersion,
                final SoftLock lock);

    boolean remove(final Object key);

    SoftLock tryLock(final Object key, final Object version);

    void unlock(final Object key, SoftLock lock);

    boolean contains(final Object key);

    void clear();

    long size();

    long getSizeInMemory();

    Map asMap();

}
