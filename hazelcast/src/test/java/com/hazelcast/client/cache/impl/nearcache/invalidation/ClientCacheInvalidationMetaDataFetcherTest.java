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
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataContainer;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.spi.CachingProvider;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;
import static com.hazelcast.cache.impl.ICacheService.SERVICE_NAME;
import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.internal.util.RandomPicker.getInt;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheInvalidationMetaDataFetcherTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void fetches_sequence_and_uuid() {
        String cacheName = "test";
        int partition = 1;
        long givenSequence = getInt(1, MAX_VALUE);
        UUID givenUuid = UuidUtil.newUnsecureUUID();

        RepairingTask repairingTask = getRepairingTask(cacheName, partition, givenSequence, givenUuid);
        InvalidationMetaDataFetcher invalidationMetaDataFetcher = repairingTask.getInvalidationMetaDataFetcher();
        ConcurrentMap<String, RepairingHandler> handlers = repairingTask.getHandlers();
        invalidationMetaDataFetcher.fetchMetadata(handlers);

        RepairingHandler repairingHandler = handlers.get(getPrefixedName(cacheName));
        MetaDataContainer metaDataContainer = repairingHandler.getMetaDataContainer(partition);

        UUID foundUuid = metaDataContainer.getUuid();
        long foundSequence = metaDataContainer.getSequence();

        assertEquals(givenSequence, foundSequence);
        assertEquals(givenUuid, foundUuid);
    }

    @SuppressWarnings("unchecked")
    private RepairingTask getRepairingTask(String cacheName, int partition, long givenSequence, UUID givenUuid) {
        HazelcastInstance member = factory.newHazelcastInstance();
        distortRandomPartitionSequence(getPrefixedName(cacheName), partition, givenSequence, member);
        distortRandomPartitionUuid(partition, givenUuid, member);

        ClientConfig clientConfig = new ClientConfig().addNearCacheConfig(new NearCacheConfig(cacheName));
        HazelcastClientProxy client = (HazelcastClientProxy) factory.newHazelcastClient(clientConfig);
        CachingProvider clientCachingProvider = createClientCachingProvider(client);
        Cache<Integer, Integer> clientCache = clientCachingProvider.getCacheManager().createCache(cacheName, newCacheConfig());

        ClientContext clientContext = ((ClientProxy) clientCache).getContext();
        return clientContext.getRepairingTask(SERVICE_NAME);
    }

    private void distortRandomPartitionSequence(String cacheName, int partition, long sequence, HazelcastInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        CacheService service = nodeEngineImpl.getService(SERVICE_NAME);
        CacheEventHandler cacheEventHandler = service.getCacheEventHandler();
        MetaDataGenerator metaDataGenerator = cacheEventHandler.getMetaDataGenerator();
        metaDataGenerator.setCurrentSequence(cacheName, partition, sequence);
    }

    private void distortRandomPartitionUuid(int partition, UUID uuid, HazelcastInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        CacheService service = nodeEngineImpl.getService(SERVICE_NAME);
        CacheEventHandler cacheEventHandler = service.getCacheEventHandler();
        MetaDataGenerator metaDataGenerator = cacheEventHandler.getMetaDataGenerator();
        metaDataGenerator.setUuid(partition, uuid);
    }

    private CacheConfig newCacheConfig() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);

        return new CacheConfig()
                .setEvictionConfig(evictionConfig);
    }

    private String getPrefixedName(String cacheName) {
        return "/hz/" + cacheName;
    }
}
