/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.internal.monitor.impl.PartitionedIndexStatsImpl;
import com.hazelcast.internal.monitor.impl.PerIndexStats;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalIndexStatsTest;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.query.LocalIndexStats;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.IndexRegistry;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.hazelcast.test.Accessors.getAllIndexes;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientIndexStatsTest extends LocalIndexStatsTest {

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.OBJECT}, {InMemoryFormat.BINARY}});
    }

    private TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private Config finalConfig;
    private HazelcastInstance member1;
    private HazelcastInstance member2;

    protected IMap map1;
    protected IMap map2;

    protected IMap noStatsMap1;
    protected IMap noStatsMap2;

    @Override
    protected HazelcastInstance createInstance(Config config) {
        finalConfig = config;
        member1 = hazelcastFactory.newHazelcastInstance(config);
        member2 = hazelcastFactory.newHazelcastInstance(config);

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

    @Override
    protected void ensureSafeState() {
        waitAllForSafeState(hazelcastFactory.getAllHazelcastInstances());
    }

    @SuppressWarnings("unchecked")
    @Test
    @Override
    public void testQueryCounting_WhenPartitionPredicateIsUsed() {
        addIndex(map, "this", false);

        for (int i = 0; i < 100; ++i) {
            map.put(i, i);
        }

        map.entrySet(Predicates.partitionPredicate(10, Predicates.equal("this", 10)));
        assertTrue(map1.getLocalMapStats().getQueryCount() == 1 && map2.getLocalMapStats().getQueryCount() == 0
                || map1.getLocalMapStats().getQueryCount() == 0 && map2.getLocalMapStats().getQueryCount() == 1);
        assertEquals(0, map1.getLocalMapStats().getIndexedQueryCount());
        assertEquals(0, map2.getLocalMapStats().getIndexedQueryCount());
        assertEquals(0, map1.getLocalMapStats().getIndexStats().get("this").getQueryCount());
        assertEquals(0, map2.getLocalMapStats().getIndexStats().get("this").getQueryCount());
    }

    @Test
    @Ignore
    @Override
    public void testAverageQuerySelectivityCalculation_WhenSomePartitionsAreEmpty() {
        // do nothing
    }

    @Test
    public void shouldUseIndexFromClient_whenMemberProxyExists() {
        testIndexWithoutMapProxy((s) -> {});
    }

    @Test
    @Ignore("HZ-4455")
    public void shouldUseIndexFromClient_whenMemberProxyDestroyed() {
        testIndexWithoutMapProxy((mapName) -> {
            member1.getMap(mapName).destroy();
        });
    }

    @Test
    public void shouldUseIndexFromClient_whenClusterRestarted() {
        warmUpPartitions(member1, member2);
        testIndexWithoutMapProxy((mapName) -> restartCluster(true, ClusterState.ACTIVE));
    }

    @Test
    public void shouldUseIndexFromClient_whenClusterRestartedForcefully() {
        warmUpPartitions(member1, member2);
        testIndexWithoutMapProxy((mapName) -> restartCluster(false, ClusterState.ACTIVE));
    }

    @Ignore("https://github.com/hazelcast/hazelcast-enterprise/issues/7063")
    @Test
    public void shouldUseIndexFromClient_whenClusterRestartedInPassiveState() {
        warmUpPartitions(member1, member2);
        testIndexWithoutMapProxy((mapName) -> restartCluster(true, ClusterState.PASSIVE));
    }

    @Test
    public void shouldUseIndexFromClient_whenClusterRestartedInFrozenState() {
        warmUpPartitions(member1, member2);
        testIndexWithoutMapProxy((mapName) -> restartCluster(true, ClusterState.FROZEN));
    }

    private void testIndexWithoutMapProxy(Consumer<String> actionBeforeTest) {
        // given map with static index
        String indexMapName = randomMapName() + "_client";
        // inherit default config
        MapConfig mapWithIndex = new MapConfig(finalConfig.getMapConfig(mapName))
                .setName(indexMapName)
                .addIndexConfig(new IndexConfig(IndexType.SORTED, "this").setName("index"));
        member1.getConfig().addMapConfig(mapWithIndex);

        var clientMap = instance.getMap(indexMapName);

        // when
        actionBeforeTest.accept(indexMapName);

        for (int i = 0; i < 100; ++i) {
            clientMap.put(i, i);
        }
        for (int i = 0; i < 100; ++i) {
            assertThat(clientMap.entrySet(Predicates.equal("this", i)))
                    .hasSize(1)
                    .containsOnly(Map.entry(i, i));
        }

        // then
        // get member maps after test not to create proxies accidentally earlier
        map1 = member1.getMap(indexMapName);
        map2 = member2.getMap(indexMapName);

        assertThat(stats().getQueryCount()).isEqualTo(100);
        assertThat(stats().getIndexedQueryCount()).isEqualTo(100);
    }

    private void restartCluster(boolean graceful, ClusterState restartInState) {
        warmUpPartitions(member1, member2);
        member1.getCluster().changeClusterState(restartInState);
        member1 = restartMember(member1, member2, graceful);
        member2 = restartMember(member2, member1, graceful);
        member1.getCluster().changeClusterState(ClusterState.ACTIVE);
    }

    private HazelcastInstance restartMember(HazelcastInstance member, HazelcastInstance otherMember, boolean graceful) {
        Address address1 = Accessors.getAddress(member);
        if (graceful) {
            member.shutdown();
        } else {
            member.getLifecycleService().terminate();
        }
        assertClusterSizeEventually(1, otherMember);
        waitAllForSafeState(otherMember);
        HazelcastInstance restartedMember = hazelcastFactory.newHazelcastInstance(address1, finalConfig);
        assertClusterSizeEventually(2, restartedMember, otherMember);
        waitAllForSafeState(restartedMember, otherMember);
        return restartedMember;
    }

    @Override
    protected LocalMapStats stats() {
        return combineStats(map1, map2);
    }

    @Override
    protected LocalMapStats noStats() {
        return combineStats(noStatsMap1, noStatsMap2);
    }

    private static LocalMapStats combineStats(IMap map1, IMap map2) {
        LocalMapStats stats1 = map1.getLocalMapStats();
        LocalMapStats stats2 = map2.getLocalMapStats();

        List<IndexRegistry> allIndexes = new ArrayList<IndexRegistry>();
        allIndexes.addAll(getAllIndexes(map1));
        allIndexes.addAll(getAllIndexes(map2));

        LocalMapStatsImpl combinedStats = new LocalMapStatsImpl();

        assertEquals(stats1.getQueryCount(), stats2.getQueryCount());
        combinedStats.setQueryCount(stats1.getQueryCount());
        assertEquals(stats1.getIndexedQueryCount(), stats2.getIndexedQueryCount());
        combinedStats.setIndexedQueryCount(stats1.getIndexedQueryCount());

        assertEquals(stats1.getIndexStats().size(), stats2.getIndexStats().size());
        Map<String, PartitionedIndexStatsImpl> combinedIndexStatsMap = new HashMap<String, PartitionedIndexStatsImpl>();
        for (Map.Entry<String, LocalIndexStats> indexEntry : stats1.getIndexStats().entrySet()) {
            LocalIndexStats indexStats1 = indexEntry.getValue();
            LocalIndexStats indexStats2 = stats2.getIndexStats().get(indexEntry.getKey());
            assertNotNull(indexStats2);

            PartitionedIndexStatsImpl combinedIndexStats = new PartitionedIndexStatsImpl();
            assertEquals(indexStats1.getHitCount(), indexStats2.getHitCount());
            combinedIndexStats.setHitCount(indexStats1.getHitCount());

            assertEquals(indexStats1.getQueryCount(), indexStats2.getQueryCount());
            combinedIndexStats.setQueryCount(indexStats1.getQueryCount());
            combinedIndexStats
                    .setAverageHitLatency((indexStats1.getAverageHitLatency() + indexStats2.getAverageHitLatency()) / 2);

            long totalHitCount = 0;
            double totalNormalizedHitCardinality = 0.0;
            for (IndexRegistry indexes : allIndexes) {
                PerIndexStats perIndexStats = indexes.getIndex(indexEntry.getKey()).getPerIndexStats();
                totalHitCount += perIndexStats.getHitCount();
                totalNormalizedHitCardinality += perIndexStats.getTotalNormalizedHitCardinality();
            }
            combinedIndexStats
                    .setAverageHitSelectivity(totalHitCount == 0 ? 0.0 : 1.0 - totalNormalizedHitCardinality / totalHitCount);

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
