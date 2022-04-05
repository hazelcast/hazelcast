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

package com.hazelcast.client.map.impl.nearcache.invalidation;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidator;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.map.impl.nearcache.NearCacheTestSupport;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.MAX_TOLERATED_MISS_COUNT;
import static com.hazelcast.internal.util.RandomPicker.getInt;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.spi.properties.ClusterProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class ClientMapInvalidationMetadataDistortionTest extends NearCacheTestSupport {

    private static final int MAP_SIZE = 100000;
    private static final String MAP_NAME = "ClientMapInvalidationMetadataDistortionTest";

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final AtomicBoolean stopTest = new AtomicBoolean();

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void lostInvalidation() {
        // members are created
        Config config = createConfig().addMapConfig(createMapConfig(MAP_NAME));
        final HazelcastInstance member = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        // map is populated form member
        final IMap<Integer, Integer> memberMap = member.getMap(MAP_NAME);
        for (int i = 0; i < MAP_SIZE; i++) {
            memberMap.put(i, i);
        }

        // a new client comes
        ClientConfig clientConfig = createClientConfig().addNearCacheConfig(createNearCacheConfig(MAP_NAME));
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        final IMap<Integer, Integer> clientMap = client.getMap(MAP_NAME);

        Thread populateNearCache = new Thread(new Runnable() {
            public void run() {
                while (!stopTest.get()) {
                    for (int i = 0; i < MAP_SIZE; i++) {
                        clientMap.get(i);
                    }
                }
            }
        });

        Thread distortSequence = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!stopTest.get()) {
                    distortRandomPartitionSequence(MAP_NAME, member);
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
                    int key = getInt(MAP_SIZE);
                    int value = getInt(Integer.MAX_VALUE);
                    memberMap.put(key, value);
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
                for (int i = 0; i < MAP_SIZE; i++) {
                    Integer valueSeenFromMember = memberMap.get(i);
                    Integer valueSeenFromClient = clientMap.get(i);

                    assertEquals(valueSeenFromMember, valueSeenFromClient);
                }
            }
        });
    }

    private Config createConfig() {
        return smallInstanceConfig()
                .setProperty(PARTITION_COUNT.getName(), "271")
                .setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true")
                .setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), "10");
    }

    private MapConfig createMapConfig(String mapName) {
        return new MapConfig(mapName)
                .setBackupCount(0);
    }

    private NearCacheConfig createNearCacheConfig(String mapName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(Integer.MAX_VALUE)
                .setEvictionPolicy(EvictionPolicy.NONE);

        return newNearCacheConfig()
                .setName(mapName)
                .setInvalidateOnChange(true)
                .setEvictionConfig(evictionConfig);
    }

    private ClientConfig createClientConfig() {
        return new ClientConfig()
                .setProperty(MAX_TOLERATED_MISS_COUNT.getName(), "0");
    }

    private void distortRandomPartitionSequence(String mapName, HazelcastInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        MapService mapService = nodeEngineImpl.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        MetaDataGenerator metaDataGenerator = invalidator.getMetaDataGenerator();
        InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        metaDataGenerator.setCurrentSequence(mapName, getInt(partitionCount), getInt(MAX_VALUE));
    }

    private void distortRandomPartitionUuid(HazelcastInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        int partitionCount = nodeEngineImpl.getPartitionService().getPartitionCount();
        int partitionId = getInt(partitionCount);
        MapService mapService = nodeEngineImpl.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        MetaDataGenerator metaDataGenerator = invalidator.getMetaDataGenerator();

        metaDataGenerator.setUuid(partitionId, UuidUtil.newUnsecureUUID());
    }
}
