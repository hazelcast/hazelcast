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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.proxy.NearCachedMapProxyImpl;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberMapReconciliationTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "test";
    private static final int RECONCILIATION_INTERVAL_SECONDS = 3;

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();
    private final Config config = new Config();

    private IMap<Integer, Integer> serverMap;

    @Before
    public void setUp() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig(MAP_NAME)
                .setInvalidateOnChange(true)
                .setCacheLocalEntries(true);

        config.setProperty("hazelcast.invalidation.max.tolerated.miss.count", "0");
        config.setProperty("hazelcast.invalidation.reconciliation.interval.seconds", valueOf(RECONCILIATION_INTERVAL_SECONDS));
        config.setProperty("hazelcast.invalidation.min.reconciliation.interval.seconds", valueOf(RECONCILIATION_INTERVAL_SECONDS));
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), valueOf(Integer.MAX_VALUE));
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), valueOf(Integer.MAX_VALUE));
        config.getMapConfig(MAP_NAME).setNearCacheConfig(nearCacheConfig);

        HazelcastInstance server = factory.newHazelcastInstance(config);
        serverMap = server.getMap(MAP_NAME);
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

        IMap<Integer, Integer> nearCachedServerMap = nearCachedMapFromNewServer();
        for (int i = 0; i < total; i++) {
            nearCachedServerMap.get(i);
        }

        IMap nearCachedMapFromNewServer = nearCachedMapFromNewServer();
        for (int i = 0; i < total; i++) {
            nearCachedMapFromNewServer.get(i);
        }

        NearCacheStats nearCacheStats = nearCachedMapFromNewServer.getLocalMapStats().getNearCacheStats();
        assertStats(nearCacheStats, total, 0, total);

        sleepSeconds(2 * RECONCILIATION_INTERVAL_SECONDS);

        for (int i = 0; i < total; i++) {
            nearCachedMapFromNewServer.get(i);
        }
        assertStats(nearCacheStats, total, total, total);
    }

    private IMap<Integer, Integer> nearCachedMapFromNewServer() {
        HazelcastInstance server = factory.newHazelcastInstance(config);
        IMap<Integer, Integer> map = server.getMap(MAP_NAME);
        assert map instanceof NearCachedMapProxyImpl;

        return map;
    }

    public static void assertStats(NearCacheStats nearCacheStats, int ownedEntryCount, int expectedHits, int expectedMisses) {
        assertEquals("not expected ownedEntryCount", ownedEntryCount, nearCacheStats.getOwnedEntryCount());
        assertEquals("not expected expectedHits", expectedHits, nearCacheStats.getHits());
        assertEquals("not expected expectedMisses", expectedMisses, nearCacheStats.getMisses());
    }
}
