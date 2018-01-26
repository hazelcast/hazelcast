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

package com.hazelcast.client.map.impl.nearcache.invalidation;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.proxy.NearCachedClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.nearcache.invalidation.MemberMapReconciliationTest.assertStats;
import static java.lang.String.valueOf;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapReconciliationTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "test";
    private static final int RECONCILIATION_INTERVAL_SECONDS = 3;

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final ClientConfig clientConfig = new ClientConfig();

    private IMap serverMap;
    private IMap clientMap;

    @Before
    public void setUp() throws Exception {
        NearCacheConfig nearCacheConfig = new NearCacheConfig(MAP_NAME);
        nearCacheConfig.setInvalidateOnChange(true);

        clientConfig.setProperty("hazelcast.invalidation.max.tolerated.miss.count", "0");
        clientConfig.setProperty("hazelcast.invalidation.reconciliation.interval.seconds", valueOf(RECONCILIATION_INTERVAL_SECONDS));
        clientConfig.setProperty("hazelcast.invalidation.min.reconciliation.interval.seconds", valueOf(RECONCILIATION_INTERVAL_SECONDS));
        clientConfig.addNearCacheConfig(nearCacheConfig);

        Config config = new Config();
        config.setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), valueOf(Integer.MAX_VALUE));
        config.setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), valueOf(Integer.MAX_VALUE));

        HazelcastInstance server = factory.newHazelcastInstance(config);
        serverMap = server.getMap(MAP_NAME);

        clientMap = createMapFromNewClient();
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    private IMap createMapFromNewClient() {
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        IMap map = client.getMap(MAP_NAME);

        assert map instanceof NearCachedClientMapProxy;

        return map;
    }

    @Test
    public void test_reconciliation_does_not_cause_premature_removal() throws Exception {
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

        sleepSeconds(2 * RECONCILIATION_INTERVAL_SECONDS);

        for (int i = 0; i < total; i++) {
            mapFromNewClient.get(i);
        }

        assertStats(nearCacheStats, total, total, total);
    }
}
