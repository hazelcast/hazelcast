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

package com.hazelcast.client.cache.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;

public abstract class ClientNearCacheTestSupport extends HazelcastTestSupport {

    protected static final String DEFAULT_CACHE_NAME = "ClientCache";

    protected final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    protected HazelcastInstance serverInstance;

    @Before
    public final void factoryInitialization() {
        serverInstance = hazelcastFactory.newHazelcastInstance(createConfig());
    }

    @After
    public final void factoryShutdown() {
        hazelcastFactory.shutdownAll();
    }

    protected Config createConfig() {
        return getBaseConfig();
    }

    protected ClientConfig createClientConfig() {
        return new ClientConfig();
    }

    @SuppressWarnings("unchecked")
    protected CacheConfig createCacheConfig(InMemoryFormat inMemoryFormat) {
        CacheConfig cacheConfig = new CacheConfig()
                .setName(DEFAULT_CACHE_NAME)
                .setInMemoryFormat(inMemoryFormat);
        cacheConfig.setCacheLoaderFactory(FactoryBuilder.factoryOf(ClientNearCacheTestSupport.TestCacheLoader.class));
        return cacheConfig;
    }

    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        return new NearCacheConfig()
                .setName(DEFAULT_CACHE_NAME)
                .setInMemoryFormat(inMemoryFormat);
    }

    protected static String generateValueFromKey(Integer key) {
        return "Value-" + key;
    }

    public static class TestCacheLoader implements CacheLoader<Integer, String> {

        @Override
        public String load(Integer key) throws CacheLoaderException {
            return String.valueOf(2 * key);
        }

        @Override
        public Map<Integer, String> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
            Map<Integer, String> entries = new HashMap<Integer, String>();
            for (int key : keys) {
                entries.put(key, String.valueOf(2 * key));
            }
            return entries;
        }
    }
}
