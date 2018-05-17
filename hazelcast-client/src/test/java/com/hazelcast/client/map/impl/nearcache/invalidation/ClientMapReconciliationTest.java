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

package com.hazelcast.client.map.impl.nearcache.invalidation;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.proxy.NearCachedClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.MAX_TOLERATED_MISS_COUNT;
import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.MIN_RECONCILIATION_INTERVAL_SECONDS;
import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.RECONCILIATION_INTERVAL_SECONDS;
import static com.hazelcast.map.impl.nearcache.invalidation.MemberMapReconciliationTest.assertStats;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapReconciliationTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "ClientMapReconciliationTest";
    private static final int RECONCILIATION_INTERVAL_SECS = 3;

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    private ClientConfig clientConfig;

    private IMap<Integer, Integer> serverMap;
    private IMap<Integer, Integer> clientMap;

    @Before
    public void setUp() {
        Config config = getBaseConfig()
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE))
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), String.valueOf(Integer.MAX_VALUE));

        NearCacheConfig nearCacheConfig = new NearCacheConfig(MAP_NAME)
                .setInvalidateOnChange(true);

        clientConfig = new ClientConfig()
                .setProperty(MAX_TOLERATED_MISS_COUNT.getName(), "0")
                .setProperty(RECONCILIATION_INTERVAL_SECONDS.getName(), String.valueOf(RECONCILIATION_INTERVAL_SECS))
                .setProperty(MIN_RECONCILIATION_INTERVAL_SECONDS.getName(), String.valueOf(RECONCILIATION_INTERVAL_SECS))
                .addNearCacheConfig(nearCacheConfig);

        HazelcastInstance server = factory.newHazelcastInstance(config);
        serverMap = server.getMap(MAP_NAME);

        clientMap = createMapFromNewClient();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void test_reconciliation_does_not_cause_premature_removal() {
        int total = 100;
        for (int i = 0; i < total; i++) {
            serverMap.put(i, i);
        }

        for (int i = 0; i < total; i++) {
            clientMap.get(i);
        }

        IMap mapFromNewClient = createMapFromNewClient();
        for (int i = 0; i < total; i++) {
            mapFromNewClient.get(i);
        }

        NearCacheStats nearCacheStats = mapFromNewClient.getLocalMapStats().getNearCacheStats();
        assertStats(nearCacheStats, total, 0, total);

        sleepSeconds(2 * RECONCILIATION_INTERVAL_SECS);

        for (int i = 0; i < total; i++) {
            mapFromNewClient.get(i);
        }

        assertStats(nearCacheStats, total, total, total);
    }

    private IMap<Integer, Integer> createMapFromNewClient() {
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        IMap<Integer, Integer> map = client.getMap(MAP_NAME);

        assertInstanceOf(NearCachedClientMapProxy.class, map);

        return map;
    }
}
