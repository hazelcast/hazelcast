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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidator;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataContainer;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.UuidUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.RandomPicker.getInt;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberMapInvalidationMetaDataFetcherTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void fetches_sequence_and_uuid() {
        String mapName = "test";
        int partition = 1;
        long givenSequence = getInt(1, Integer.MAX_VALUE);
        UUID givenUuid = UuidUtil.newUnsecureUUID();

        RepairingTask repairingTask = getRepairingTask(mapName, partition, givenSequence, givenUuid);
        InvalidationMetaDataFetcher invalidationMetaDataFetcher = repairingTask.getInvalidationMetaDataFetcher();
        ConcurrentMap<String, RepairingHandler> handlers = repairingTask.getHandlers();
        invalidationMetaDataFetcher.fetchMetadata(handlers);

        RepairingHandler repairingHandler = handlers.get(mapName);
        MetaDataContainer metaDataContainer = repairingHandler.getMetaDataContainer(partition);
        UUID foundUuid = metaDataContainer.getUuid();
        long foundSequence = metaDataContainer.getSequence();

        assertEquals(givenSequence, foundSequence);
        assertEquals(givenUuid, foundUuid);
    }

    private RepairingTask getRepairingTask(String mapName, int partition, long givenSequence, UUID givenUuid) {
        Config config = getBaseConfig();
        config.getMapConfig(mapName).setNearCacheConfig(new NearCacheConfig());

        HazelcastInstance member = factory.newHazelcastInstance(config);
        MapService mapService = getNodeEngineImpl(member).getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();

        distortRandomPartitionSequence(mapName, partition, givenSequence, member);
        distortRandomPartitionUuid(partition, givenUuid, member);

        member.getMap(mapName);
        return mapServiceContext.getMapNearCacheManager().getRepairingTask();
    }

    private void distortRandomPartitionSequence(String mapName, int partition, long sequence, HazelcastInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        MapService mapService = nodeEngineImpl.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        MetaDataGenerator metaDataGenerator = invalidator.getMetaDataGenerator();
        metaDataGenerator.setCurrentSequence(mapName, partition, sequence);
    }

    private void distortRandomPartitionUuid(int partition, UUID uuid, HazelcastInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        MapService mapService = nodeEngineImpl.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        MetaDataGenerator metaDataGenerator = invalidator.getMetaDataGenerator();
        metaDataGenerator.setUuid(partition, uuid);
    }
}
