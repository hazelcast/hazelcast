/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.namespace.icache;

import com.hazelcast.cache.CacheTestSupport;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.AbstractHazelcastCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.internal.namespace.UCDTest;

public abstract class ICacheUCDTest extends UCDTest {
    protected CacheSimpleConfig cacheConfig;
    protected ICache<Integer, Integer> cache;

    @Override
    protected void initialiseConfig() {
        cacheConfig = new CacheSimpleConfig(objectName);
        cacheConfig.setNamespace(getNamespaceName());
    }

    @Override
    protected void initialiseDataStructure() throws Exception {
        final AbstractHazelcastCachingProvider cachingProvider;

        switch (connectionStyle) {
            case CLIENT_TO_MEMBER:
                cachingProvider = CacheTestSupport.createClientCachingProvider(instance);
                break;
            case EMBEDDED:
            case MEMBER_TO_MEMBER:
                cachingProvider = CacheTestSupport.createServerCachingProvider(instance);
                break;
            default:
                throw new IllegalArgumentException(connectionStyle.toString());

        }

        cachingProvider.getCacheManager().createCache(objectName, new CacheConfig<>(cacheConfig));
        cache = instance.getCacheManager().getCache(objectName);
    }

    @Override
    protected void registerConfig(Config config) {
        config.addCacheConfig(cacheConfig);
    }
}
