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

package com.hazelcast.map.impl.nearcache.invalidation;

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
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.MAX_TOLERATED_MISS_COUNT;
import static com.hazelcast.internal.util.RandomPicker.getInt;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class MemberMapInvalidationMetadataDistortionTest extends NearCacheTestSupport {

    private static final int MAP_SIZE = 100000;
    private static final String MAP_NAME = "MemberMapInvalidationMetadataDistortionTest";

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();
    private final AtomicBoolean stopTest = new AtomicBoolean();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void lostInvalidation() {
        // members are created
        MapConfig mapConfig = createMapConfig(MAP_NAME);
        Config config = createConfig().addMapConfig(mapConfig);
        final HazelcastInstance member = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        // map is populated form member
        final IMap<Integer, Integer> memberMap = member.getMap(MAP_NAME);
        for (int i = 0; i < MAP_SIZE; i++) {
            memberMap.put(i, i);
        }

        // a new member comes with Near Cache configured
        MapConfig nearCachedMapConfig = createMapConfig(MAP_NAME).setNearCacheConfig(createNearCacheConfig(MAP_NAME));
        Config nearCachedConfig = createConfig().addMapConfig(nearCachedMapConfig);
        HazelcastInstance nearCachedMember = factory.newHazelcastInstance(nearCachedConfig);
        final IMap<Integer, Integer> nearCachedMap = nearCachedMember.getMap(MAP_NAME);

        Thread populateNearCache = new Thread(new Runnable() {
            public void run() {
                while (!stopTest.get()) {
                    for (int i = 0; i < MAP_SIZE; i++) {
                        nearCachedMap.get(i);
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
                    sleepSeconds(3);
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
                    sleepAtLeastMillis(10);
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
                    Integer valueSeenFromNearCachedSide = nearCachedMap.get(i);

                    assertEquals(valueSeenFromMember, valueSeenFromNearCachedSide);
                }
            }
        });
    }

    private Config createConfig() {
        return smallInstanceConfig()
                .setProperty(MAX_TOLERATED_MISS_COUNT.getName(), "0")
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true")
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), "10000")
                .setProperty(PARTITION_COUNT.getName(), "271");
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
