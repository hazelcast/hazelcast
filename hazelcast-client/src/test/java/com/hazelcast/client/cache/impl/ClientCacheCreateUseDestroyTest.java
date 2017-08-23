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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.impl.CacheCreateUseDestroyTest;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;

import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

import static com.hazelcast.cache.HazelcastCachingProvider.propertiesByInstanceItself;

public class ClientCacheCreateUseDestroyTest extends CacheCreateUseDestroyTest {

    private TestHazelcastFactory factory;

    @Before
    @Override
    public void setup() {
        assumptions();
        factory = new TestHazelcastFactory();
        HazelcastInstance member = factory.newHazelcastInstance(getConfig());
        CachingProvider provider = Caching.getCachingProvider();
        defaultCacheManager = provider.getCacheManager(null, null, propertiesByInstanceItself(factory.newHazelcastClient()));
        cacheService = getNode(member).getNodeEngine().getService(ICacheService.SERVICE_NAME);
        CacheEntryListenerFactory.listener = null;
    }

    @After
    public void cleanup() {
        if (factory != null) {
            factory.terminateAll();
        }
        Caching.getCachingProvider().close();
    }
}
