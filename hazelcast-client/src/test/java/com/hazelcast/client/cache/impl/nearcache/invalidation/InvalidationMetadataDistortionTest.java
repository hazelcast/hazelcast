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

package com.hazelcast.client.cache.impl.nearcache.invalidation;

import com.hazelcast.cache.impl.CacheEventHandler;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.cache.impl.nearcache.ClientNearCacheTestSupport;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.util.UuidUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.util.RandomPicker.getInt;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class InvalidationMetadataDistortionTest extends ClientNearCacheTestSupport {

    @Override
    protected Config createConfig() {
        Config config = super.createConfig();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "271");
        config.setProperty(GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true");
        config.setProperty(GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), "10");
        return config;
    }

    protected ClientConfig createClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty("hazelcast.invalidation.max.tolerated.miss.count", "0");
        return clientConfig;
    }

    @Override
    protected CacheConfig createCacheConfig(InMemoryFormat inMemoryFormat) {
        CacheConfig cacheConfig = super.createCacheConfig(inMemoryFormat);
        cacheConfig.getEvictionConfig()
                .setMaximumSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);
        return cacheConfig;
    }


    @Override
    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = super.createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setInvalidateOnChange(true)
                .getEvictionConfig()
                .setMaximumSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);
        return nearCacheConfig;
    }

    @Test
    public void ensure_nearCachedClient_and_member_data_sync_eventually() throws Exception {
        final int cacheSize = 100000;
        final AtomicBoolean stopTest = new AtomicBoolean();

        final Config config = createConfig();
        final HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);

        CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(serverInstance);
        CacheManager serverCacheManager = provider.getCacheManager();

        // populated from member.
        final Cache<Integer, Integer> memberCache = serverCacheManager.createCache(DEFAULT_CACHE_NAME, createCacheConfig(BINARY));
        for (int i = 0; i < cacheSize; i++) {
            memberCache.put(i, i);
        }

        ClientConfig clientConfig = createClientConfig();
        clientConfig.addNearCacheConfig(createNearCacheConfig(BINARY));
        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        CachingProvider clientCachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);

        final Cache<Integer, Integer> clientCache = clientCachingProvider.getCacheManager().createCache(
                DEFAULT_CACHE_NAME, createCacheConfig(BINARY));

        Thread populateNearCache = new Thread(new Runnable() {
            public void run() {
                while (!stopTest.get()) {
                    for (int i = 0; i < cacheSize; i++) {
                        clientCache.get(i);
                    }
                }
            }
        });

        Thread distortSequence = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!stopTest.get()) {
                    distortRandomPartitionSequence(DEFAULT_CACHE_NAME, member);
                    sleepSeconds(1);
                }
            }
        });

        Thread distortUuid = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!stopTest.get()) {
                    distortRandomPartitionUuid(member);
                    sleepSeconds(5);
                }
            }
        });


        Thread put = new Thread(new Runnable() {
            public void run() {
                // change some data
                while (!stopTest.get()) {
                    int key = getInt(cacheSize);
                    int value = getInt(Integer.MAX_VALUE);
                    memberCache.put(key, value);
                    sleepAtLeastMillis(100);
                }
            }
        });

        // start threads
        put.start();
        populateNearCache.start();
        distortSequence.start();
        distortUuid.start();

        sleepSeconds(60);

        // stop threads
        stopTest.set(true);
        distortUuid.join();
        distortSequence.join();
        populateNearCache.join();
        put.join();


        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < cacheSize; i++) {
                    Integer valueSeenFromMember = memberCache.get(i);
                    Integer valueSeenFromClient = clientCache.get(i);

                    assertEquals(valueSeenFromMember, valueSeenFromClient);
                }
            }
        });
    }

    private void distortRandomPartitionSequence(String mapName, HazelcastInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        CacheService service = nodeEngineImpl.getService(CacheService.SERVICE_NAME);
        CacheEventHandler cacheEventHandler = service.getCacheEventHandler();
        MetaDataGenerator metaDataGenerator = cacheEventHandler.getMetaDataGenerator();
        InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();
        int partitionCount = partitionService.getPartitionCount();

        int randomPartition = getInt(partitionCount);
        int randomSequence = getInt(MAX_VALUE);
        metaDataGenerator.setCurrentSequence(mapName, randomPartition, randomSequence);
    }

    private void distortRandomPartitionUuid(HazelcastInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        int partitionCount = nodeEngineImpl.getPartitionService().getPartitionCount();
        CacheService service = nodeEngineImpl.getService(CacheService.SERVICE_NAME);
        CacheEventHandler cacheEventHandler = service.getCacheEventHandler();
        MetaDataGenerator metaDataGenerator = cacheEventHandler.getMetaDataGenerator();

        UUID uuid = UuidUtil.newUnsecureUUID();
        int randomPartition = getInt(partitionCount);
        metaDataGenerator.setUuid(randomPartition, uuid);
    }

    protected HazelcastClientInstanceImpl getHazelcastClientInstanceImpl(HazelcastInstance client) {
        HazelcastClientProxy clientProxy = (HazelcastClientProxy) client;
        return clientProxy.client;
    }
}
