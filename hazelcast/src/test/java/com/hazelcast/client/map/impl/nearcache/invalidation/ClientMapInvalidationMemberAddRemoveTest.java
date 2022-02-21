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
import com.hazelcast.client.map.impl.nearcache.NearCachedClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.nearcache.NearCacheTestSupport;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.MAX_TOLERATED_MISS_COUNT;
import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.RECONCILIATION_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static com.hazelcast.internal.util.RandomPicker.getInt;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(NightlyTest.class)
public class ClientMapInvalidationMemberAddRemoveTest extends NearCacheTestSupport {

    private static final int TEST_RUN_SECONDS = 30;
    private static final int KEY_COUNT = 1000;
    private static final int INVALIDATION_BATCH_SIZE = 100;
    private static final int RECONCILIATION_INTERVAL_SECS = 30;
    private static final int NEAR_CACHE_POPULATE_THREAD_COUNT = 5;

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void ensure_nearCachedClient_and_member_data_sync_eventually() {
        final String mapName = "ClientMapInvalidationMemberAddRemoveTest";
        final AtomicBoolean stopTest = new AtomicBoolean();

        // members are created
        final Config config = createConfig();

        HazelcastInstance member = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        // map is populated form member
        final IMap<Integer, Integer> memberMap = member.getMap(mapName);
        for (int i = 0; i < KEY_COUNT; i++) {
            memberMap.put(i, i);
        }

        // a new client comes
        ClientConfig clientConfig = createClientConfig().addNearCacheConfig(createNearCacheConfig(mapName));
        final HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        ArrayList<Thread> threads = new ArrayList<Thread>();

        // continuously adds and removes member
        Thread shadowMember = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!stopTest.get()) {
                    HazelcastInstance member = factory.newHazelcastInstance(config);
                    sleepSeconds(5);
                    member.getLifecycleService().terminate();
                }

            }
        });
        threads.add(shadowMember);

        // populates client Near Cache
        for (int i = 0; i < NEAR_CACHE_POPULATE_THREAD_COUNT; i++) {
            Thread populateClientNearCache = new Thread(new Runnable() {
                public void run() {
                    int i = 0;
                    while (!stopTest.get()) {
                        clientMap.get(i++);
                        if (i == KEY_COUNT) {
                            i = 0;
                        }
                    }
                }
            });

            threads.add(populateClientNearCache);
        }

        // updates map data from member
        Thread putFromMember = new Thread(new Runnable() {
            public void run() {
                while (!stopTest.get()) {
                    int key = getInt(KEY_COUNT);
                    int value = getInt(Integer.MAX_VALUE);
                    memberMap.put(key, value);

                    sleepAtLeastMillis(2);
                }
            }
        });
        threads.add(putFromMember);

        Thread clearFromMember = new Thread(new Runnable() {
            public void run() {
                while (!stopTest.get()) {
                    memberMap.clear();
                    sleepSeconds(5);
                }
            }
        });
        threads.add(clearFromMember);

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
                    Integer valueSeenFromMember = memberMap.get(i);
                    Integer valueSeenFromClient = clientMap.get(i);

                    int nearCacheSize = ((NearCachedClientMapProxy) clientMap).getNearCache().size();

                    assertEquals("Stale value found. (nearCacheSize=" + nearCacheSize + ")",
                            valueSeenFromMember, valueSeenFromClient);
                }
            }
        });
    }

    protected Config createConfig() {
        return smallInstanceConfig()
                .setProperty(PARTITION_COUNT.getName(), "271")
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true")
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), String.valueOf(INVALIDATION_BATCH_SIZE));
    }

    protected ClientConfig createClientConfig() {
        return new ClientConfig()
                .setProperty(MAX_TOLERATED_MISS_COUNT.getName(), "0")
                .setProperty(RECONCILIATION_INTERVAL_SECONDS.getName(), String.valueOf(RECONCILIATION_INTERVAL_SECS));
    }

    protected NearCacheConfig createNearCacheConfig(String mapName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(Integer.MAX_VALUE)
                .setEvictionPolicy(EvictionPolicy.NONE);

        return newNearCacheConfig()
                .setName(mapName)
                .setInvalidateOnChange(true)
                .setEvictionConfig(evictionConfig);
    }
}
