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

package com.hazelcast.client.map;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.LocalIndexStatsTest;
import com.hazelcast.monitor.LocalIndexStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.impl.LocalIndexStatsImpl;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientIndexStatsTest extends LocalIndexStatsTest {

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.OBJECT}, {InMemoryFormat.BINARY}});
    }

    private TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    protected IMap map1;
    protected IMap map2;

    protected IMap noStatsMap1;
    protected IMap noStatsMap2;

    @Override
    protected HazelcastInstance createInstance(Config config) {
        HazelcastInstance member1 = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance member2 = hazelcastFactory.newHazelcastInstance(config);

        map1 = member1.getMap(mapName);
        map2 = member2.getMap(mapName);

        noStatsMap1 = member1.getMap(noStatsMapName);
        noStatsMap2 = member2.getMap(noStatsMapName);

        return hazelcastFactory.newHazelcastClient(new ClientConfig());
    }

    @After
    public void after() {
        hazelcastFactory.terminateAll();
    }

    @SuppressWarnings("unchecked")
    @Test
    @Override
    public void testQueryCounting_WhenPartitionPredicateIsUsed() {
        map.addIndex("this", false);

        for (int i = 0; i < 100; ++i) {
            map.put(i, i);
        }

        map.entrySet(new PartitionPredicate(10, Predicates.equal("this", 10)));
        assertTrue(map1.getLocalMapStats().getQueryCount() == 1 && map2.getLocalMapStats().getQueryCount() == 0
                || map1.getLocalMapStats().getQueryCount() == 0 && map2.getLocalMapStats().getQueryCount() == 1);
        assertEquals(0, map1.getLocalMapStats().getIndexedQueryCount());
        assertEquals(0, map2.getLocalMapStats().getIndexedQueryCount());
        assertEquals(0, map1.getLocalMapStats().getIndexStats().get("this").getQueryCount());
        assertEquals(0, map2.getLocalMapStats().getIndexStats().get("this").getQueryCount());
    }

    @Override
    protected LocalMapStats stats() {
        LocalMapStats stats1 = map1.getLocalMapStats();
        LocalMapStats stats2 = map2.getLocalMapStats();
        return combineStats(stats1, stats2);
    }

    @Override
    protected LocalMapStats noStats() {
        LocalMapStats stats1 = noStatsMap1.getLocalMapStats();
        LocalMapStats stats2 = noStatsMap2.getLocalMapStats();
        return combineStats(stats1, stats2);
    }

    private static LocalMapStats combineStats(LocalMapStats stats1, LocalMapStats stats2) {
        LocalMapStatsImpl combinedStats = new LocalMapStatsImpl();

        assertEquals(stats1.getQueryCount(), stats2.getQueryCount());
        combinedStats.setQueryCount(stats1.getQueryCount());
        assertEquals(stats1.getIndexedQueryCount(), stats2.getIndexedQueryCount());
        combinedStats.setIndexedQueryCount(stats1.getIndexedQueryCount());

        assertEquals(stats1.getIndexStats().size(), stats2.getIndexStats().size());
        Map<String, LocalIndexStatsImpl> combinedIndexStatsMap = new HashMap<String, LocalIndexStatsImpl>();
        for (Map.Entry<String, LocalIndexStats> indexEntry : stats1.getIndexStats().entrySet()) {
            LocalIndexStats indexStats1 = indexEntry.getValue();
            LocalIndexStats indexStats2 = stats2.getIndexStats().get(indexEntry.getKey());
            assertNotNull(indexStats2);

            LocalIndexStatsImpl combinedIndexStats = new LocalIndexStatsImpl();
            assertEquals(indexStats1.getHitCount(), indexStats2.getHitCount());
            combinedIndexStats.setHitCount(indexStats1.getHitCount());

            assertEquals(indexStats1.getQueryCount(), indexStats2.getQueryCount());
            combinedIndexStats.setQueryCount(indexStats1.getQueryCount());
            combinedIndexStats.setAverageHitSelectivity(
                    (indexStats1.getAverageHitSelectivity() + indexStats2.getAverageHitSelectivity()) / 2.0);
            combinedIndexStats
                    .setAverageHitLatency((indexStats1.getAverageHitLatency() + indexStats2.getAverageHitLatency()) / 2);

            combinedIndexStats.setInsertCount(indexStats1.getInsertCount() + indexStats2.getInsertCount());
            combinedIndexStats.setTotalInsertLatency(indexStats1.getTotalInsertLatency() + indexStats2.getTotalInsertLatency());

            combinedIndexStats.setUpdateCount(indexStats1.getUpdateCount() + indexStats2.getUpdateCount());
            combinedIndexStats.setTotalUpdateLatency(indexStats1.getTotalUpdateLatency() + indexStats2.getTotalUpdateLatency());

            combinedIndexStats.setRemoveCount(indexStats1.getRemoveCount() + indexStats2.getRemoveCount());
            combinedIndexStats.setTotalRemoveLatency(indexStats1.getTotalRemoveLatency() + indexStats2.getTotalRemoveLatency());

            combinedIndexStats.setMemoryCost(indexStats1.getMemoryCost() + indexStats2.getMemoryCost());

            combinedIndexStatsMap.put(indexEntry.getKey(), combinedIndexStats);
        }
        combinedStats.setIndexStats(combinedIndexStatsMap);

        return combinedStats;
    }

}
