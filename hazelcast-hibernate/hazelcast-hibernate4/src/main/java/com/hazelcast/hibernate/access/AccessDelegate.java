/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.hibernate.access;

import com.hazelcast.hibernate.region.HazelcastRegion;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.SoftLock;

/**
 * This interface is used to implement basic transactional guarantees
 *
 * @author Leo Kim (lkim@limewire.com)
 * @param <T> implementation type of HazelcastRegion
 */
public interface AccessDelegate<T extends HazelcastRegion> {

    /**
     * Get the wrapped cache region
     *
     * @return The underlying region
     */
    T getHazelcastRegion();

    /**
     * Attempt to retrieve an object from the cache. Mainly used in attempting
     * to resolve entities/collections from the second level cache.
     *
     * @param key         The key of the item to be retrieved.
     * @param txTimestamp a timestamp prior to the transaction start time
     * @return the cached object or <tt>null</tt>
     * @throws org.hibernate.cache.CacheException
     *          Propogated from underlying {@link org.hibernate.cache.spi.Region}
     */
    Object get(Object key, long txTimestamp) throws CacheException;

    /**
     * Called after an item has been inserted (before the transaction completes),
     * instead of calling evict().
     * This method is used by "synchronous" concurrency strategies.
     *
     * @param key     The item key
     * @param value   The item
     * @param version The item's version value
     * @return Were the contents of the cache actual changed by this operation?
     * @throws org.hibernate.cache.CacheException
     *          Propogated from underlying {@link org.hibernate.cache.spi.Region}
     */
    boolean insert(Object key, Object value, Object version) throws CacheException;

    /**
     * Called after an item has been inserted (after the transaction completes),
     * instead of calling release().
     * This method is used by "asynchronous" concurrency strategies.
     *
     * @param key     The item key
     * @param value   The item
     * @param version The item's version value
     * @return Were the contents of the cache actual changed by this operation?
     * @throws org.hibernate.cache.CacheException
     *          Propogated from underlying {@link org.hibernate.cache.spi.Region}
     */
    boolean afterInsert(Object key, Object value, Object version) throws CacheException;

    /**
     * Called after an item has been updated (before the transaction completes),
     * instead of calling evict(). This method is used by "synchronous" concurrency
     * strategies.
     *
     * @param key             The item key
     * @param value           The item
     * @param currentVersion  The item's current version value
     * @param previousVersion The item's previous version value
     * @return Were the contents of the cache actual changed by this operation?
     * @throws org.hibernate.cache.CacheException
     *          Propogated from underlying {@link org.hibernate.cache.spi.Region}
     */
    boolean update(Object key, Object value, Object currentVersion, Object previousVersion) throws CacheException;

    /**
     * Called after an item has been updated (after the transaction completes),
     * instead of calling release().  This method is used by "asynchronous"
     * concurrency strategies.
     *
     * @param key             The item key
     * @param value           The item
     * @param currentVersion  The item's current version value
     * @param previousVersion The item's previous version value
     * @param lock            The lock previously obtained from {@link #lockItem}
     * @return Were the contents of the cache actual changed by this operation?
     * @throws org.hibernate.cache.CacheException
     *          Propogated from underlying {@link org.hibernate.cache.spi.Region}
     */
    boolean afterUpdate(Object key, Object value, Object currentVersion, Object previousVersion, SoftLock lock)
            throws CacheException;

    /**
     * Attempt to cache an object, after loading from the database.
     *
     * @param key         The item key
     * @param value       The item
     * @param txTimestamp a timestamp prior to the transaction start time
     * @param version     the item version number
     * @return <tt>true</tt> if the object was successfully cached
     * @throws org.hibernate.cache.CacheException
     *          Propogated from underlying {@link org.hibernate.cache.spi.Region}
     */
    boolean putFromLoad(Object key, Object value, long txTimestamp, Object version) throws CacheException;

    /**
     * Attempt to cache an object, after loading from the database, explicitly
     * specifying the minimalPut behavior.
     *
     * @param key                The item key
     * @param value              The item
     * @param txTimestamp        a timestamp prior to the transaction start time
     * @param version            the item version number
     * @param minimalPutOverride Explicit minimalPut flag
     * @return <tt>true</tt> if the object was successfully cached
     * @throws org.hibernate.cache.CacheException
     *          Propogated from underlying {@link org.hibernate.cache.spi.Region}
     */
    boolean putFromLoad(Object key, Object value, long txTimestamp, Object version, boolean minimalPutOverride)
            throws CacheException;

    /**
     * Called after an item has become stale (before the transaction completes).
     * This method is used by "synchronous" concurrency strategies.
     *
     * @param key The key of the item to remove
     * @throws org.hibernate.cache.CacheException
     *          Propogated from underlying {@link org.hibernate.cache.spi.Region}
     */
    void remove(Object key) throws CacheException;

    /**
     * Called to evict data from the entire region
     *
     * @throws org.hibernate.cache.CacheException
     *          Propogated from underlying {@link org.hibernate.cache.spi.Region}
     */
    void removeAll() throws CacheException;

    /**
     * Forcibly evict an item from the cache immediately without regard for transaction
     * isolation.
     *
     * @param key The key of the item to remove
     * @throws org.hibernate.cache.CacheException
     *          Propogated from underlying {@link org.hibernate.cache.spi.Region}
     */
    void evict(Object key) throws CacheException;

    /**
     * Forcibly evict all items from the cache immediately without regard for transaction
     * isolation.
     *
     * @throws org.hibernate.cache.CacheException
     *          Propogated from underlying {@link org.hibernate.cache.spi.Region}
     */
    void evictAll() throws CacheException;

    /**
     * We are going to attempt to update/delete the keyed object. This
     * method is used by "asynchronous" concurrency strategies.
     * <p/>
     * The returned object must be passed back to release(), to release the
     * lock. Concurrency strategies which do not support client-visible
     * locks may silently return null.
     *
     * @param key     The key of the item to lock
     * @param version The item's current version value
     * @return A representation of our lock on the item; or null.
     * @throws org.hibernate.cache.CacheException
     *          Propogated from underlying {@link org.hibernate.cache.spi.Region}
     */
    SoftLock lockItem(Object key, Object version) throws CacheException;

    /**
     * Lock the entire region
     *
     * @return A representation of our lock on the item; or null.
     * @throws org.hibernate.cache.CacheException
     *          Propogated from underlying {@link org.hibernate.cache.spi.Region}
     */
    SoftLock lockRegion() throws CacheException;

    /**
     * Called when we have finished the attempted update/delete (which may or
     * may not have been successful), after transaction completion.  This method
     * is used by "asynchronous" concurrency strategies.
     *
     * @param key  The item key
     * @param lock The lock previously obtained from {@link #lockItem}
     * @throws org.hibernate.cache.CacheException
     *          Propogated from underlying {@link org.hibernate.cache.spi.Region}
     */
    void unlockItem(Object key, SoftLock lock) throws CacheException;

    /**
     * Called after we have finished the attempted invalidation of the entire
     * region
     *
     * @param lock The lock previously obtained from {@link #lockRegion}
     * @throws org.hibernate.cache.CacheException
     *          Propogated from underlying {@link org.hibernate.cache.spi.Region}
     */
    void unlockRegion(SoftLock lock) throws CacheException;
}
