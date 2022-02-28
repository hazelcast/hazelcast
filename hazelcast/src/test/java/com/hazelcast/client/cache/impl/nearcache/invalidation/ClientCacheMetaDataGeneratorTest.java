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

package com.hazelcast.client.cache.impl.nearcache.invalidation;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheEventHandler;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.cache.impl.nearcache.NearCachedClientCacheProxy;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;
import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("WeakerAccess")
public class ClientCacheMetaDataGeneratorTest extends HazelcastTestSupport {

    private static final String CACHE_NAME = "CacheMetaDataGeneratorTest";
    private static final String PREFIXED_CACHE_NAME = "/hz/" + CACHE_NAME;

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    private ClientConfig clientConfig;
    private CacheConfig<Integer, Integer> cacheConfig;

    private ICache<Integer, Integer> serverCache;
    private HazelcastInstance server;

    @Before
    public void setUp() {
        NearCacheConfig nearCacheConfig = getNearCacheConfig();

        clientConfig = getClientConfig()
                .addNearCacheConfig(nearCacheConfig);

        cacheConfig = getCacheConfig(CACHE_NAME);

        Config config = getConfig();
        server = factory.newHazelcastInstance(config);

        CachingProvider provider = createServerCachingProvider(server);
        CacheManager serverCacheManager = provider.getCacheManager();

        serverCache = (ICache<Integer, Integer>) serverCacheManager.createCache(CACHE_NAME, cacheConfig);
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void destroying_cache_removes_related_metadata_when_near_cache_exists() {
        ICache<Integer, Integer> clientCache = createCacheFromNewClient();

        serverCache.put(1, 1);

        assertNotNull(getMetaDataGenerator(server).getSequenceGenerators().get(PREFIXED_CACHE_NAME));

        clientCache.destroy();

        assertNull(getMetaDataGenerator(server).getSequenceGenerators().get(PREFIXED_CACHE_NAME));
    }

    @Test
    public void destroying_cache_removes_related_metadata_when_near_cache_not_exists() {
        serverCache.put(1, 1);

        assertNull(getMetaDataGenerator(server).getSequenceGenerators().get(PREFIXED_CACHE_NAME));

        serverCache.destroy();

        assertNull(getMetaDataGenerator(server).getSequenceGenerators().get(PREFIXED_CACHE_NAME));
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    protected CacheConfig<Integer, Integer> getCacheConfig(String cacheName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);

        return new CacheConfig<Integer, Integer>()
                .setName(cacheName)
                .setEvictionConfig(evictionConfig);
    }

    protected NearCacheConfig getNearCacheConfig() {
        return new NearCacheConfig(CACHE_NAME)
                .setInvalidateOnChange(true);
    }

    private ICache<Integer, Integer> createCacheFromNewClient() {
        HazelcastClientProxy client = (HazelcastClientProxy) factory.newHazelcastClient(clientConfig);
        CachingProvider clientCachingProvider = createClientCachingProvider(client);

        CacheManager cacheManager = clientCachingProvider.getCacheManager();
        ICache<Integer, Integer> cache = (ICache<Integer, Integer>) cacheManager.createCache(CACHE_NAME, cacheConfig);

        assertInstanceOf(NearCachedClientCacheProxy.class, cache);

        return cache;
    }

    private static MetaDataGenerator getMetaDataGenerator(HazelcastInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        CacheService service = nodeEngineImpl.getService(CacheService.SERVICE_NAME);
        CacheEventHandler cacheEventHandler = service.getCacheEventHandler();
        return cacheEventHandler.getMetaDataGenerator();
    }
}
