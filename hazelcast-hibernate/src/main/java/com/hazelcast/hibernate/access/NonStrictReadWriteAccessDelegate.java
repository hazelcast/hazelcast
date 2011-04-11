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

import org.hibernate.cache.CacheException;
import org.hibernate.cache.access.SoftLock;

import com.hazelcast.hibernate.region.HazelcastRegion;

/**
 * Makes no guarantee of consistency between the cache and the database. Stale data from the cache is possible if expiry
 * is not configured appropriately.
 *
 * @author Leo Kim (lkim@limewire.com)
 */
public class NonStrictReadWriteAccessDelegate<T extends HazelcastRegion> extends AbstractAccessDelegate<T> {

	public NonStrictReadWriteAccessDelegate(T hazelcastRegion) {
		super(hazelcastRegion);
	}
    
	public boolean afterInsert(final Object key, final Object value, final Object version) throws CacheException {
		getCache().put(key, value);
		return true;
	}

	public boolean afterUpdate(final Object key, final Object value, final Object currentVersion, final Object previousVersion,
			final SoftLock lock) throws CacheException {
		getCache().put(key, value);
		return true;
	}

	public boolean putFromLoad(final Object key, final Object value, final long txTimestamp, final Object version,
			final boolean minimalPutOverride) throws CacheException {
		getCache().put(key, value);
		return true;
	}

	public void remove(final Object key) throws CacheException {
		getCache().remove(key);
	}

	public SoftLock lockItem(Object key, Object version) throws CacheException {
		return null;
	}
	
	public void unlockItem(final Object key, final SoftLock lock) throws CacheException {
		remove(key);
	}

	public void unlockRegion(final SoftLock lock) throws CacheException {
		removeAll();
	}
}
