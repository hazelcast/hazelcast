/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.hibernate.access;

import com.hazelcast.core.IMap;
import com.hazelcast.hibernate.region.HazelcastRegion;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.access.SoftLock;

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
