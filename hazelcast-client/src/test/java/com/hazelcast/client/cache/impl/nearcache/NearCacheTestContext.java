/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.client.cache.impl.nearcache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

/**
 * Context for client Near Cache tests.
 */
public class NearCacheTestContext {

    protected final HazelcastClientProxy client;
    protected final HazelcastInstance member;
    protected final SerializationService serializationService;
    protected final HazelcastClientCacheManager cacheManager;
    protected final HazelcastServerCacheManager memberCacheManager;
    protected final NearCacheManager nearCacheManager;
    protected final ICache<Object, String> cache;
    protected final ICache<Object, String> memberCache;
    protected final NearCache<Data, String> nearCache;

    NearCacheTestContext(HazelcastClientProxy client, HazelcastClientCacheManager cacheManager,
                         NearCacheManager nearCacheManager, ICache<Object, String> cache,
                         NearCache<Data, String> nearCache) {
        this.client = client;
        this.member = null;
        this.serializationService = client.getSerializationService();
        this.cacheManager = cacheManager;
        this.memberCacheManager = null;
        this.nearCacheManager = nearCacheManager;
        this.cache = cache;
        this.memberCache = null;
        this.nearCache = nearCache;
    }

    NearCacheTestContext(HazelcastClientProxy client, HazelcastInstance member,
                         HazelcastClientCacheManager cacheManager, HazelcastServerCacheManager memberCacheManager,
                         NearCacheManager nearCacheManager,
                         ICache<Object, String> cache, ICache<Object, String> memberCache,
                         NearCache<Data, String> nearCache) {
        this.client = client;
        this.member = member;
        this.serializationService = client.getSerializationService();
        this.cacheManager = cacheManager;
        this.memberCacheManager = memberCacheManager;
        this.nearCacheManager = nearCacheManager;
        this.cache = cache;
        this.memberCache = memberCache;
        this.nearCache = nearCache;
    }
}
