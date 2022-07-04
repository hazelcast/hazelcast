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
import com.hazelcast.client.cache.impl.nearcache.NearCachedClientCacheProxy;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.nearcache.NearCacheRecordStore;
import com.hazelcast.internal.nearcache.impl.DefaultNearCache;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataContainer;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.internal.nearcache.impl.invalidation.StaleReadDetector;
import com.hazelcast.internal.nearcache.impl.store.AbstractNearCacheRecordStore;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;
import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.INVALIDATE;
import static com.hazelcast.internal.nearcache.impl.invalidation.InvalidationUtils.NO_SEQUENCE;
import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.MAX_TOLERATED_MISS_COUNT;
import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.RECONCILIATION_INTERVAL_SECONDS;
import static com.hazelcast.internal.util.RandomPicker.getInt;
import static com.hazelcast.spi.properties.ClusterProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getPartitionService;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class ClientCacheInvalidationMemberAddRemoveTest extends ClientNearCacheTestSupport {

    private static final int TEST_RUN_SECONDS = 30;
    private static final int KEY_COUNT = 1000;
    private static final int INVALIDATION_BATCH_SIZE = 100;
    private static final int RECONCILIATION_INTERVAL_SECS = 30;
    private static final int NEAR_CACHE_POPULATE_THREAD_COUNT = 5;

    private HazelcastInstance secondNode;

    @Parameters(name = "localUpdatePolicy:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {INVALIDATE},
                {CACHE_ON_UPDATE}
        });
    }

    @Parameter
    public LocalUpdatePolicy localUpdatePolicy;

    @Test
    public void ensure_nearCachedClient_and_member_data_sync_eventually() {
        final AtomicBoolean stopTest = new AtomicBoolean();

        final Config config = createConfig();
        secondNode = hazelcastFactory.newHazelcastInstance(config);

        CachingProvider provider = createServerCachingProvider(serverInstance);
        final CacheManager serverCacheManager = provider.getCacheManager();

        // populated from member
        final Cache<Integer, Integer> memberCache = serverCacheManager.createCache(DEFAULT_CACHE_NAME, createCacheConfig(BINARY));
        for (int i = 0; i < KEY_COUNT; i++) {
            memberCache.put(i, i);
        }

        ClientConfig clientConfig = createClientConfig()
                .addNearCacheConfig(createNearCacheConfig(BINARY));
        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        CachingProvider clientCachingProvider = createClientCachingProvider(client);

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

        for (int i = 0; i < NEAR_CACHE_POPULATE_THREAD_COUNT; i++) {
            // populates client Near Cache
            Thread populateClientNearCache = new Thread(new Runnable() {
                public void run() {
                    int i = 0;
                    while (!stopTest.get()) {
                        clientCache.get(i++);
                        if (i == KEY_COUNT) {
                            i = 0;
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

        // stop threads
        stopTest.set(true);
        for (Thread thread : threads) {
            assertJoinable(thread);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < KEY_COUNT; i++) {
                    Integer valueSeenFromMember = memberCache.get(i);
                    Integer valueSeenFromClient = clientCache.get(i);

                    String msg = createFailureMessage(i);
                    assertEquals(msg, valueSeenFromMember, valueSeenFromClient);
                }
            }

            @SuppressWarnings("unchecked")
            private String createFailureMessage(int i) {
                int partitionId = getPartitionService(serverInstance).getPartitionId(i);

                NearCacheRecordStore nearCacheRecordStore = getNearCacheRecordStore();
                NearCacheRecord record = nearCacheRecordStore.getRecord(i);
                long recordSequence = record == null ? NO_SEQUENCE : record.getInvalidationSequence();

                // member-1
                MetaDataGenerator metaDataGenerator1 = getMetaDataGenerator(serverInstance);
                long memberSequence1 = metaDataGenerator1.currentSequence("/hz/" + DEFAULT_CACHE_NAME, partitionId);
                UUID memberUuid1 = metaDataGenerator1.getUuidOrNull(partitionId);

                // member-2
                MetaDataGenerator metaDataGenerator2 = getMetaDataGenerator(secondNode);
                long memberSequence2 = metaDataGenerator2.currentSequence("/hz/" + DEFAULT_CACHE_NAME, partitionId);
                UUID memberUuid2 = metaDataGenerator2.getUuidOrNull(partitionId);

                StaleReadDetector staleReadDetector = getStaleReadDetector(nearCacheRecordStore);
                MetaDataContainer metaDataContainer = staleReadDetector.getMetaDataContainer(partitionId);
                return format("On client: [uuid=%s, partition=%d, onRecordSequence=%d, latestSequence=%d, staleSequence=%d],"
                                + "%nOn members: [memberUuid1=%s, memberSequence1=%d, memberUuid2=%s, memberSequence2=%d]",
                        metaDataContainer.getUuid(), partitionId, recordSequence, metaDataContainer.getSequence(),
                        metaDataContainer.getStaleSequence(), memberUuid1, memberSequence1, memberUuid2, memberSequence2);
            }

            private MetaDataGenerator getMetaDataGenerator(HazelcastInstance node) {
                CacheService service = getNodeEngineImpl(node).getService(CacheService.SERVICE_NAME);
                CacheEventHandler cacheEventHandler = service.getCacheEventHandler();
                return cacheEventHandler.getMetaDataGenerator();
            }

            private NearCacheRecordStore getNearCacheRecordStore() {
                NearCache nearCache = ((NearCachedClientCacheProxy) clientCache).getNearCache();
                DefaultNearCache defaultNearCache = (DefaultNearCache) nearCache.unwrap(DefaultNearCache.class);
                return defaultNearCache.getNearCacheRecordStore();
            }
        });
    }

    protected StaleReadDetector getStaleReadDetector(NearCacheRecordStore nearCacheRecordStore) {
        return ((AbstractNearCacheRecordStore) nearCacheRecordStore).getStaleReadDetector();
    }

    @Override
    protected Config createConfig() {
        return super.createConfig()
                .setProperty(PARTITION_COUNT.getName(), "271")
                .setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true")
                .setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), String.valueOf(INVALIDATION_BATCH_SIZE));
    }

    @Override
    protected ClientConfig createClientConfig() {
        return new ClientConfig()
                .setProperty(MAX_TOLERATED_MISS_COUNT.getName(), "0")
                .setProperty(RECONCILIATION_INTERVAL_SECONDS.getName(), String.valueOf(RECONCILIATION_INTERVAL_SECS));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected CacheConfig<Integer, Integer> createCacheConfig(InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(Integer.MAX_VALUE);

        return (CacheConfig<Integer, Integer>) super.createCacheConfig(inMemoryFormat)
                .setEvictionConfig(evictionConfig);
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(Integer.MAX_VALUE);

        return super.createNearCacheConfig(inMemoryFormat)
                .setInvalidateOnChange(true)
                .setLocalUpdatePolicy(localUpdatePolicy)
                .setEvictionConfig(evictionConfig);
    }
}
