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

package com.hazelcast.client.map.impl.nearcache.invalidation;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.nearcache.NearCacheTestSupport;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.util.RandomPicker.getInt;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvalidationMemberAddRemoveTest extends NearCacheTestSupport {

    protected final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void ensure_nearCachedClient_and_member_data_sync_eventually() throws Exception {
        final String mapName = "origin-map";
        final int mapSize = 100000;
        final AtomicBoolean stopTest = new AtomicBoolean();

        // members are created.
        final Config config = createConfig().addMapConfig(createMapConfig(mapName));
        HazelcastInstance member = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        // map is populated form member.
        final IMap<Integer, Integer> memberMap = member.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            memberMap.put(i, i);
        }

        // a new client comes.
        ClientConfig clientConfig = createClientConfig().addNearCacheConfig(createNearCacheConfig(mapName));
        final HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        final IMap<Integer, Integer> clientMap = client.getMap(mapName);

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
                        clientMap.get(i);
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

        // start threads
        putFromMember.start();
        populateClientNearCache.start();
        shadowMember.start();

        // stress system some seconds
        sleepSeconds(60);

        //stop threads
        stopTest.set(true);
        shadowMember.join();
        populateClientNearCache.join();
        putFromMember.join();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = 0; i < mapSize; i++) {
                    Integer valueSeenFromMember = memberMap.get(i);
                    Integer valueSeenFromClient = clientMap.get(i);

                    assertEquals(valueSeenFromMember, valueSeenFromClient);
                }
            }
        });
    }

    protected Config createConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "271");
        config.setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true");
        config.setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), "10");
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

    protected ClientConfig createClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty("hazelcast.invalidation.max.tolerated.miss.count", "0");
        return clientConfig;
    }
}
