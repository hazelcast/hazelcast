/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.nearcache.NearCacheTestSupport;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.util.RandomPicker.getInt;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class InvalidationMemberAddRemoveTest extends NearCacheTestSupport {

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    protected Config createConfig() {
        Config config = new Config();
        config.setProperty("hazelcast.invalidation.max.tolerated.miss.count", "0");
        config.setProperty("hazelcast.map.invalidation.batch.enabled", "true");
        config.setProperty("hazelcast.map.invalidation.batch.size", "10000");
        config.setProperty("hazelcast.partition.count", "271");
        return config;
    }

    protected MapConfig createMapConfig(String mapName) {
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setBackupCount(0);
        return mapConfig;
    }

    protected NearCacheConfig createNearCacheConfig(String mapName) {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        nearCacheConfig.setName(mapName);
        nearCacheConfig.getEvictionConfig()
                .setSize(Integer.MAX_VALUE)
                .setEvictionPolicy(EvictionPolicy.NONE);
        return nearCacheConfig;
    }

    @Test
    public void ensure_nearCached_and_actual_data_sync_eventually() throws Exception {
        final String mapName = "origin-map";
        final int mapSize = 100000;
        final AtomicBoolean stopTest = new AtomicBoolean();

        // members are created.
        final Config config = createConfig().addMapConfig(createMapConfig(mapName));
        final HazelcastInstance member = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        // map is populated form member.
        final IMap<Integer, Integer> memberMap = member.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            memberMap.put(i, i);
        }

        // a new member comes with near-cache configured.
        final Config config2 = createConfig().addMapConfig(createMapConfig(mapName).setNearCacheConfig(createNearCacheConfig(mapName)));
        final HazelcastInstance nearCachedMember = factory.newHazelcastInstance(config2);
        final IMap<Integer, Integer> nearCachedMap = nearCachedMember.getMap(mapName);

        // continuously adds and removes member
        Thread shadowMember = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!stopTest.get()) {
                    HazelcastInstance member = factory.newHazelcastInstance(config);
                    sleepSeconds(5);
                    member.getLifecycleService().shutdown();
                }

            }
        });

        // populates client near-cache
        Thread populateClientNearCache = new Thread(new Runnable() {
            public void run() {
                while (!stopTest.get()) {
                    for (int i = 0; i < mapSize; i++) {
                        nearCachedMap.get(i);
                    }
                }
            }
        });

        // updates map data from member.
        Thread putFromMember = new Thread(new Runnable() {
            public void run() {
                while (!stopTest.get()) {
                    int key = getInt(mapSize);
                    int value = getInt(Integer.MAX_VALUE);
                    memberMap.put(key, value);

                    sleepAtLeastMillis(5);
                }
            }
        });

        Thread clearFromMember = new Thread(new Runnable() {
            public void run() {
                while (!stopTest.get()) {
                    memberMap.clear();
                    sleepSeconds(5);
                }
            }
        });

        // start threads
        putFromMember.start();
        populateClientNearCache.start();
        clearFromMember.start();
        shadowMember.start();

        // stress system some seconds
        sleepSeconds(60);

        //stop threads
        stopTest.set(true);
        shadowMember.join();
        clearFromMember.start();
        populateClientNearCache.join();
        putFromMember.join();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < mapSize; i++) {
                    Integer valueSeenFromMember = memberMap.get(i);
                    Integer valueSeenFromClient = nearCachedMap.get(i);

                    assertEquals(valueSeenFromMember, valueSeenFromClient);
                }
            }
        });
    }

}