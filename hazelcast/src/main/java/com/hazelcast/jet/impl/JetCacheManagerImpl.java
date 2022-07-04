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

package com.hazelcast.jet.impl;

import com.hazelcast.cache.ICache;
import com.hazelcast.jet.JetCacheManager;
import com.hazelcast.jet.JetInstance;

/**
 * Hazelcast Jet {@code JetCacheManager} implementation accessible via {@code JetInstance} that provides access to JSR-107
 * (JCache) caches configured on the cluster via Hazelcast API.
 *
 * @see JetCacheManager
 */
public class JetCacheManagerImpl implements JetCacheManager {

    private final JetInstance jetInstance;

    JetCacheManagerImpl(JetInstance jetInstance) {
        this.jetInstance = jetInstance;
    }

    @Override
    public <K, V> ICache<K, V> getCache(String name) {
        return CacheGetter.getCache(jetInstance, name);
    }

    /**
     * This class is necessary to conceal the cache-api
     */
    private static class CacheGetter {

        private static <K, V> ICache<K, V> getCache(JetInstance jetInstance, String name) {
            return new ICacheDecorator<>(
                    jetInstance.getHazelcastInstance().getCacheManager().getCache(name), jetInstance
            );
        }

    }
}
