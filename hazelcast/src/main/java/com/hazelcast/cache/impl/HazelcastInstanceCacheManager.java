/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.ICache;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ICacheManager;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.exception.ServiceNotFoundException;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Hazelcast {@code ICacheManager} implementation accessible via {@code HazelcastInstance} that provides access to JSR-107
 * (JCache) caches configured on the cluster via Hazelcast API.
 *
 * @see com.hazelcast.core.ICacheManager
 */
public class HazelcastInstanceCacheManager implements ICacheManager {

    private final HazelcastInstanceImpl original;

    public HazelcastInstanceCacheManager(HazelcastInstanceImpl original) {
        this.original = original;
    }

    @Override
    public <K, V> ICache<K, V> getCache(String name) {
        checkNotNull(name, "Retrieving a cache instance with a null name is not allowed!");
        return getCacheByFullName(HazelcastCacheManager.CACHE_MANAGER_PREFIX + name);
    }

    public <K, V> ICache<K, V> getCacheByFullName(String fullName) {
        checkNotNull(fullName, "Retrieving a cache instance with a null name is not allowed!");
        try {
            return original.getDistributedObject(ICacheService.SERVICE_NAME, fullName);
        } catch (HazelcastException e) {
            if (e.getCause() instanceof ServiceNotFoundException) {
                throw new IllegalStateException(ICacheService.CACHE_SUPPORT_NOT_AVAILABLE_ERROR_MESSAGE);
            } else {
                throw e;
            }
        }
    }
}
