/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.core.HazelcastInstance;

import javax.cache.CacheManager;

/**
 * Contract point of Hazelcast cache manager implementations (client + server) based on {@link CacheManager}.
 *
 * @see CacheManager
 */
public interface HazelcastCacheManager
        extends CacheManager {

    /**
     * Hazelcast's {@link javax.cache.CacheManager}'s prefix to be appended to the head of the cache name.
     */
    String CACHE_MANAGER_PREFIX = "/hz/";

    /**
     * Gets cache name by adding manager prefix.
     *
     * @param name pure cache name with prefix
     * @return the cache name with manager prefix
     */
    String getCacheNameWithPrefix(String name);

    /**
     * Gets the underlying {@link HazelcastInstance} implementation.
     *
     * @return the underlying {@link HazelcastInstance} implementation
     */
    HazelcastInstance getHazelcastInstance();

    /**
     * Destroys cache manager.
     */
    void destroy();

}
