/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cache.impl.nearcache.invalidation;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheEventHandler;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.cache.impl.NearCachedClientCacheProxy;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheMetaDataGeneratorTest extends HazelcastTestSupport {

    private static final String CACHE_NAME = "test";
    private static final String PREFIXED_CACHE_NAME = "/hz/" + CACHE_NAME;

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final ClientConfig clientConfig = newClientConfig();
    private final CacheConfig cacheConfig = newCacheConfig();

    private Cache clientCache;
    private Cache serverCache;
    private HazelcastInstance server;

    @Before
    public void setUp() throws Exception {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        clientConfig.addNearCacheConfig(nearCacheConfig);

        cacheConfig.getEvictionConfig()
                .setMaximumSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);
        cacheConfig.setName(CACHE_NAME);

        Config config = getConfig();
        server = factory.newHazelcastInstance(config);

        CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(server);
        CacheManager serverCacheManager = provider.getCacheManager();

        serverCache = serverCacheManager.createCache(CACHE_NAME, cacheConfig);
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void destroying_cache_removes_related_metadata_when_near_cache_exists() throws Exception {
        clientCache = createCacheFromNewClient();

        serverCache.put(1, 1);

        assertNotNull(getMetaDataGenerator(server).getSequenceGenerators().get(PREFIXED_CACHE_NAME));

        ((ICache) clientCache).destroy();

        assertNull(getMetaDataGenerator(server).getSequenceGenerators().get(PREFIXED_CACHE_NAME));
    }

    @Test
    public void destroying_cache_removes_related_metadata_when_near_cache_not_exists() throws Exception {
        serverCache.put(1, 1);

        assertNull(getMetaDataGenerator(server).getSequenceGenerators().get(PREFIXED_CACHE_NAME));

        ((ICache) serverCache).destroy();

        assertNull(getMetaDataGenerator(server).getSequenceGenerators().get(PREFIXED_CACHE_NAME));
    }

    private Cache createCacheFromNewClient() {
        HazelcastClientProxy client = (HazelcastClientProxy) factory.newHazelcastClient(clientConfig);
        CachingProvider clientCachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);

        CacheManager cacheManager = clientCachingProvider.getCacheManager();
        Cache cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        assert cache instanceof NearCachedClientCacheProxy;

        return cache;
    }

    protected ClientConfig newClientConfig() {
        return new ClientConfig();
    }

    protected CacheConfig newCacheConfig() {
        return new CacheConfig();
    }

    protected NearCacheConfig newNearCacheConfig() {
        return new NearCacheConfig(CACHE_NAME);
    }

    private static MetaDataGenerator getMetaDataGenerator(HazelcastInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        CacheService service = nodeEngineImpl.getService(CacheService.SERVICE_NAME);
        CacheEventHandler cacheEventHandler = service.getCacheEventHandler();
        return cacheEventHandler.getMetaDataGenerator();
    }
}
