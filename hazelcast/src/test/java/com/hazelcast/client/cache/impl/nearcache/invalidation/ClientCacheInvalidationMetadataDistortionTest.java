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

import com.hazelcast.cache.impl.CacheEventHandler;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.cache.impl.nearcache.ClientNearCacheTestSupport;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;
import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.MAX_TOLERATED_MISS_COUNT;
import static com.hazelcast.internal.util.RandomPicker.getInt;
import static com.hazelcast.spi.properties.ClusterProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class ClientCacheInvalidationMetadataDistortionTest extends ClientNearCacheTestSupport {

    private static final int CACHE_SIZE = 100000;

    private final AtomicBoolean stopTest = new AtomicBoolean();

    @Test
    public void ensure_nearCachedClient_and_member_data_sync_eventually() {
        final Config config = createConfig();
        final HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);

        CachingProvider provider = createServerCachingProvider(serverInstance);
        CacheManager serverCacheManager = provider.getCacheManager();

        // populated from member
        final Cache<Integer, Integer> memberCache = serverCacheManager.createCache(DEFAULT_CACHE_NAME, createCacheConfig(BINARY));
        for (int i = 0; i < CACHE_SIZE; i++) {
            memberCache.put(i, i);
        }

        ClientConfig clientConfig = createClientConfig();
        clientConfig.addNearCacheConfig(createNearCacheConfig(BINARY));
        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        CachingProvider clientCachingProvider = createClientCachingProvider(client);

        final Cache<Integer, Integer> clientCache = clientCachingProvider.getCacheManager().createCache(
                DEFAULT_CACHE_NAME, createCacheConfig(BINARY));

        Thread populateNearCache = new Thread(new Runnable() {
            public void run() {
                while (!stopTest.get()) {
                    for (int i = 0; i < CACHE_SIZE; i++) {
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
                    int key = getInt(CACHE_SIZE);
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
        assertJoinable(distortUuid, distortSequence, populateNearCache, put);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < CACHE_SIZE; i++) {
                    Integer valueSeenFromMember = memberCache.get(i);
                    Integer valueSeenFromClient = clientCache.get(i);

                    assertEquals(valueSeenFromMember, valueSeenFromClient);
                }
            }
        });
    }

    @Override
    protected Config createConfig() {
        return super.createConfig()
                .setProperty(PARTITION_COUNT.getName(), "271")
                .setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true")
                .setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), "10");
    }

    @Override
    protected ClientConfig createClientConfig() {
        return new ClientConfig()
                .setProperty(MAX_TOLERATED_MISS_COUNT.getName(), "0");
    }

    @Override
    @SuppressWarnings("unchecked")
    protected CacheConfig<Integer, Integer> createCacheConfig(InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);

        CacheConfig<Integer, Integer> cacheConfig = super.createCacheConfig(inMemoryFormat);
        cacheConfig.setEvictionConfig(evictionConfig);
        return cacheConfig;
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);

        return super.createNearCacheConfig(inMemoryFormat)
                .setInvalidateOnChange(true)
                .setEvictionConfig(evictionConfig);
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
}
