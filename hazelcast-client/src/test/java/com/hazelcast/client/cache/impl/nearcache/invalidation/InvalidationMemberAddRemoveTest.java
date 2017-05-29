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
import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.cache.impl.NearCachedClientCacheProxy;
import com.hazelcast.client.cache.impl.nearcache.ClientNearCacheTestSupport;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.nearcache.NearCacheRecordStore;
import com.hazelcast.internal.nearcache.impl.DefaultNearCache;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataContainer;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.internal.nearcache.impl.invalidation.InvalidationUtils.NO_SEQUENCE;
import static com.hazelcast.util.RandomPicker.getInt;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class InvalidationMemberAddRemoveTest extends ClientNearCacheTestSupport {

    private static final int NEAR_CACHE_POPULATOR_THREAD_COUNT = 5;
    private static final int TEST_RUN_SECONDS = 30;
    private static final int INVALIDATION_BATCH_SIZE = 100;
    private static final int KEY_COUNT = 1000;
    private static final int RECONCILIATION_INTERVAL_SECONDS = 30;

    @Override
    protected Config createConfig() {
        Config config = super.createConfig();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "271");
        config.setProperty(GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true");
        config.setProperty(GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), Integer.toString(INVALIDATION_BATCH_SIZE));
        return config;
    }

    protected ClientConfig createClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty("hazelcast.invalidation.max.tolerated.miss.count", "0");
        clientConfig.setProperty("hazelcast.invalidation.reconciliation.interval.seconds", Integer.toString(RECONCILIATION_INTERVAL_SECONDS));
        return clientConfig;
    }

    @Test
    public void ensure_nearCachedClient_and_member_data_sync_eventually() throws Exception {
        final AtomicBoolean stopTest = new AtomicBoolean();

        final Config config = createConfig();
        hazelcastFactory.newHazelcastInstance(config);

        CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(serverInstance);
        final CacheManager serverCacheManager = provider.getCacheManager();

        // populated from member
        final Cache<Integer, Integer> memberCache = serverCacheManager.createCache(DEFAULT_CACHE_NAME, createCacheConfig(BINARY));
        for (int i = 0; i < KEY_COUNT; i++) {
            memberCache.put(i, i);
        }

        ClientConfig clientConfig = createClientConfig();
        clientConfig.addNearCacheConfig(createNearCacheConfig(BINARY));
        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        CachingProvider clientCachingProvider = HazelcastClientCachingProvider.createCachingProvider(client);

        final Cache<Integer, Integer> clientCache = clientCachingProvider.getCacheManager().createCache(
                DEFAULT_CACHE_NAME, createCacheConfig(BINARY));

        ArrayList<Thread> threads = new ArrayList<Thread>();

        // continuously adds and removes member
        Thread shadowMember = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!stopTest.get()) {
                    HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
                    sleepSeconds(5);
                    member.getLifecycleService().terminate();
                }
            }
        });

        threads.add(shadowMember);

        for (int i = 0; i < NEAR_CACHE_POPULATOR_THREAD_COUNT; i++) {
            // populates client Near Cache
            Thread populateClientNearCache = new Thread(new Runnable() {
                public void run() {
                    while (!stopTest.get()) {
                        for (int i = 0; i < KEY_COUNT; i++) {
                            clientCache.get(i);
                        }
                    }
                }
            });
            threads.add(populateClientNearCache);
        }

        // updates data from member
        Thread putFromMember = new Thread(new Runnable() {
            public void run() {
                while (!stopTest.get()) {
                    int key = getInt(KEY_COUNT);
                    int value = getInt(Integer.MAX_VALUE);
                    memberCache.put(key, value);

                    sleepAtLeastMillis(2);
                }
            }
        });
        threads.add(putFromMember);

        Thread clearFromMember = new Thread(new Runnable() {
            public void run() {
                while (!stopTest.get()) {
                    memberCache.clear();
                    sleepSeconds(3);
                }
            }
        });
        threads.add(clearFromMember);

        // start threads
        for (Thread thread : threads) {
            thread.start();
        }

        // stress system some seconds
        sleepSeconds(TEST_RUN_SECONDS);

        //stop threads
        stopTest.set(true);
        for (Thread thread : threads) {
            thread.join();
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < KEY_COUNT; i++) {
                    Integer valueSeenFromMember = memberCache.get(i);
                    Integer valueSeenFromClient = clientCache.get(i);
                    if (valueSeenFromMember != null && valueSeenFromClient == null) {
                        System.err.println("found");
                        valueSeenFromClient = clientCache.get(i);
                    }

                    String msg = createFailureMessage(i);
                    assertEquals(msg, valueSeenFromMember, valueSeenFromClient);
                }
            }

            private String createFailureMessage(int i) {
                InternalPartitionService partitionService = getPartitionService(serverInstance);
                Data keyData = getSerializationService(serverInstance).toData(i);
                int partitionId = partitionService.getPartitionId(keyData);

                NearCacheRecordStore nearCacheRecordStore = getNearCacheRecordStore();
                NearCacheRecord record = nearCacheRecordStore.getRecord(keyData);
                long recordSequence = record == null ? NO_SEQUENCE : record.getInvalidationSequence();

                MetaDataGenerator metaDataGenerator = getMetaDataGenerator();
                long memberSequence = metaDataGenerator.currentSequence("/hz/" + DEFAULT_CACHE_NAME, partitionId);

                StaleReadDetector staleReadDetector = nearCacheRecordStore.getStaleReadDetector();
                MetaDataContainer metaDataContainer = staleReadDetector.getMetaDataContainer(partitionId);
                return String.format("partition=%d, onRecordSequence=%d, latestSequence=%d, staleSequence=%d, memberSequence=%d",
                        partitionService.getPartitionId(keyData), recordSequence, metaDataContainer.getSequence(),
                        metaDataContainer.getStaleSequence(), memberSequence);
            }

            private MetaDataGenerator getMetaDataGenerator() {
                CacheEventHandler cacheEventHandler = ((CacheService) ((CacheProxy) memberCache).getService()).getCacheEventHandler();
                return cacheEventHandler.getMetaDataGenerator();
            }

            private NearCacheRecordStore getNearCacheRecordStore() {
                NearCache nearCache = ((NearCachedClientCacheProxy) clientCache).getNearCache();
                DefaultNearCache defaultNearCache = (DefaultNearCache) nearCache.unwrap(DefaultNearCache.class);
                return defaultNearCache.getNearCacheRecordStore();
            }
        });
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
}
