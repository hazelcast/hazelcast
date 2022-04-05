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

package com.hazelcast.map;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.LocalIndexStats;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalIndexStatsTest extends HazelcastTestSupport {

    @Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.OBJECT}, {InMemoryFormat.BINARY}});
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    protected static final int PARTITIONS = 137;

    private static final int QUERIES = 10;

    private QueryType[] queryTypes;

    protected String mapName;

    protected String noStatsMapName;

    protected HazelcastInstance instance;

    protected IMap<Integer, Integer> map;

    protected IMap<Integer, Integer> noStatsMap;

    @Before
    public void before() {
        mapName = randomMapName();
        noStatsMapName = mapName + "_no_stats";

        Config config = getConfig();
        config.setProperty(PARTITION_COUNT.getName(), Integer.toString(PARTITIONS));
        config.getMapConfig(mapName).setInMemoryFormat(inMemoryFormat);
        config.getMapConfig(noStatsMapName).setStatisticsEnabled(false);
        config.getMetricsConfig().setEnabled(false);

        instance = createInstance(config);
        map = instance.getMap(mapName);
        noStatsMap = instance.getMap(noStatsMapName);
        queryTypes = initQueryTypes();
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    protected HazelcastInstance createInstance(Config config) {
        return createHazelcastInstance(config);
    }

    @Test
    public void testQueryCounting() {
        addIndex(map, "this", false);
        for (int i = 0; i < 100; ++i) {
            map.put(i, i);
        }

        long expectedQueryCount = 0;
        long expectedIndexedQueryCount = 0;

        assertEquals(expectedQueryCount, stats().getQueryCount());
        assertEquals(expectedIndexedQueryCount, stats().getIndexedQueryCount());
        for (QueryType queryType : queryTypes) {
            query(queryType, Predicates.alwaysTrue(), Predicates.equal("this", 10));

            expectedQueryCount += QUERIES * 2;
            if (queryType.isIndexed()) {
                expectedIndexedQueryCount += QUERIES;
            }

            assertEquals(expectedQueryCount, stats().getQueryCount());
            assertEquals(expectedIndexedQueryCount, stats().getIndexedQueryCount());
        }
    }

    @Test
    public void testHitAndQueryCounting_WhenAllIndexesHit() {
        addIndex(map, "__key", false);
        addIndex(map, "this", true);
        for (int i = 0; i < 100; ++i) {
            map.put(i, i);
        }

        long expectedKeyHitCount = 0;
        long expectedKeyQueryCount = 0;
        long expectedValueHitCount = 0;
        long expectedValueQueryCount = 0;

        assertEquals(expectedKeyHitCount, keyStats().getHitCount());
        assertEquals(expectedKeyQueryCount, keyStats().getQueryCount());
        assertEquals(expectedValueHitCount, valueStats().getHitCount());
        assertEquals(expectedValueQueryCount, valueStats().getQueryCount());
        for (QueryType queryType : queryTypes) {
            query(queryType, Predicates.alwaysTrue(), Predicates.equal("__key", 10), Predicates.equal("this", 10));

            if (queryType.isIndexed()) {
                expectedKeyHitCount += QUERIES;
                expectedKeyQueryCount += QUERIES;
                expectedValueHitCount += QUERIES;
                expectedValueQueryCount += QUERIES;
            }

            assertEquals(expectedKeyHitCount, keyStats().getHitCount());
            assertEquals(expectedKeyQueryCount, keyStats().getQueryCount());
            assertEquals(expectedValueHitCount, valueStats().getHitCount());
            assertEquals(expectedValueQueryCount, valueStats().getQueryCount());
        }
    }

    @Test
    public void testHitAndQueryCounting_WhenSingleIndexHit() {
        addIndex(map, "__key", false);
        addIndex(map, "this", true);
        for (int i = 0; i < 100; ++i) {
            map.put(i, i);
        }

        long expectedKeyHitCount = 0;
        long expectedKeyQueryCount = 0;

        assertEquals(expectedKeyHitCount, keyStats().getHitCount());
        assertEquals(expectedKeyQueryCount, keyStats().getQueryCount());
        assertEquals(0, valueStats().getHitCount());
        assertEquals(0, valueStats().getQueryCount());
        for (QueryType queryType : queryTypes) {
            query(queryType, Predicates.alwaysTrue(), Predicates.equal("__key", 10));

            if (queryType.isIndexed()) {
                expectedKeyHitCount += QUERIES;
                expectedKeyQueryCount += QUERIES;
            }

            assertEquals(expectedKeyHitCount, keyStats().getHitCount());
            assertEquals(expectedKeyQueryCount, keyStats().getQueryCount());
            assertEquals(0, valueStats().getHitCount());
            assertEquals(0, valueStats().getQueryCount());
        }
    }

    @Test
    public void testHitCounting_WhenIndexHitMultipleTimes() {
        addIndex(map, "__key", false);
        addIndex(map, "this", true);
        for (int i = 0; i < 100; ++i) {
            map.put(i, i);
        }

        long expectedKeyHitCount = 0;
        long expectedKeyQueryCount = 0;
        long expectedValueHitCount = 0;
        long expectedValueQueryCount = 0;

        assertEquals(expectedKeyHitCount, keyStats().getHitCount());
        assertEquals(expectedKeyQueryCount, keyStats().getQueryCount());
        assertEquals(expectedValueHitCount, valueStats().getHitCount());
        assertEquals(expectedValueQueryCount, valueStats().getQueryCount());
        for (QueryType queryType : queryTypes) {
            query(queryType, Predicates.alwaysTrue(), Predicates.or(Predicates.equal("__key", 10), Predicates.equal("__key", 20)),
                    Predicates.equal("this", 10));

            if (queryType.isIndexed()) {
                expectedKeyHitCount += QUERIES * 2;
                expectedKeyQueryCount += QUERIES;
                expectedValueHitCount += QUERIES;
                expectedValueQueryCount += QUERIES;
            }

            assertEquals(expectedKeyHitCount, keyStats().getHitCount());
            assertEquals(expectedKeyQueryCount, keyStats().getQueryCount());
            assertEquals(expectedValueHitCount, valueStats().getHitCount());
            assertEquals(expectedValueQueryCount, valueStats().getQueryCount());
        }
    }

    @Test
    public void testAverageQuerySelectivityCalculation_WhenSomePartitionsAreEmpty() {
        testAverageQuerySelectivityCalculation(100);
    }

    @Test
    public void testAverageQuerySelectivityCalculation_WhenAllPartitionsArePopulated() {
        testAverageQuerySelectivityCalculation(1000);
    }

    @Test
    public void testAverageQuerySelectivityCalculation_WhenAllPartitionsAreHeavilyPopulated() {
        testAverageQuerySelectivityCalculation(10000);
    }

    private void testAverageQuerySelectivityCalculation(int entryCount) {
        double expected = 1.0 - 1.0 / entryCount;

        addIndex(map, "__key", false);
        addIndex(map, "this", true);
        for (int i = 0; i < entryCount; ++i) {
            map.put(i, i);
        }
        assertEquals(0.0, keyStats().getAverageHitSelectivity(), 0.0);
        assertEquals(0.0, valueStats().getAverageHitSelectivity(), 0.0);

        for (int i = 0; i < QUERIES; ++i) {
            map.entrySet(Predicates.equal("__key", 10));
            map.entrySet(Predicates.equal("this", 10));
            assertEquals(expected, keyStats().getAverageHitSelectivity(), 0.015);
            assertEquals(expected, valueStats().getAverageHitSelectivity(), 0.015);
        }

        for (int i = 1; i <= QUERIES; ++i) {
            map.entrySet(Predicates.greaterEqual("__key", entryCount / 2));
            map.entrySet(Predicates.greaterEqual("this", entryCount / 2));
            assertEquals((expected * QUERIES + 0.5 * i) / (QUERIES + i), keyStats().getAverageHitSelectivity(), 0.015);
            assertEquals((expected * QUERIES + 0.5 * i) / (QUERIES + i), valueStats().getAverageHitSelectivity(), 0.015);
        }
    }

    @Test
    public void testAverageQuerySelectivityCalculation_ChangingNumberOfIndex() {
        double expected1 = 1.0 - 0.001;
        double expected2 = 1.0 - 0.1;
        double expected3 = 1.0 - 0.4;

        addIndex(map, "__key", false);
        addIndex(map, "this", true);
        for (int i = 0; i < 1000; ++i) {
            map.put(i, i);
        }
        assertEquals(0.0, keyStats().getAverageHitSelectivity(), 0.0);
        assertEquals(0.0, valueStats().getAverageHitSelectivity(), 0.0);

        for (int i = 0; i < QUERIES; ++i) {
            map.entrySet(Predicates.equal("__key", 10));
            map.entrySet(Predicates.equal("this", 10));
            assertEquals(expected1, keyStats().getAverageHitSelectivity(), 0.015);
            assertEquals(expected1, valueStats().getAverageHitSelectivity(), 0.015);
        }

        for (int i = 1000; i < 2000; ++i) {
            map.put(i, i);
        }

        for (int i = 1; i <= QUERIES; ++i) {
            map.entrySet(Predicates.greaterEqual("__key", 1800));
            map.entrySet(Predicates.greaterEqual("this", 1800));
            assertEquals((expected1 * QUERIES + expected2 * i) / (QUERIES + i), keyStats().getAverageHitSelectivity(), 0.015);
            assertEquals((expected1 * QUERIES + expected2 * i) / (QUERIES + i), valueStats().getAverageHitSelectivity(), 0.015);
        }

        for (int i = 1500; i < 2000; ++i) {
            map.remove(i);
        }

        for (int i = 1; i <= QUERIES; ++i) {
            map.entrySet(Predicates.greaterEqual("__key", 900));
            map.entrySet(Predicates.greaterEqual("this", 900));
            assertEquals(((expected1 + expected2) * QUERIES + expected3 * i) / (2 * QUERIES + i),
                    keyStats().getAverageHitSelectivity(), 0.015);
            assertEquals(((expected1 + expected2) * QUERIES + expected3 * i) / (2 * QUERIES + i),
                    valueStats().getAverageHitSelectivity(), 0.015);
        }
    }

    @Test
    public void testQueryCounting_WhenTwoMapsUseIndexesNamedTheSame() {
        addIndex(map, "__key", false);
        addIndex(map, "this", true);

        IMap<Integer, Integer> otherMap = instance.getMap(map.getName() + "_other_map");
        addIndex(otherMap, "__key", false);
        addIndex(otherMap, "this", true);

        for (int i = 0; i < 100; ++i) {
            otherMap.put(i, i);
        }

        otherMap.entrySet(Predicates.equal("__key", 10));
        assertEquals(0, keyStats().getQueryCount());
        assertEquals(0, valueStats().getQueryCount());

        map.entrySet(Predicates.equal("__key", 10));
        assertEquals(1, keyStats().getQueryCount());
        assertEquals(0, valueStats().getQueryCount());
    }

    @Test
    public void testQueryCounting_WhenPartitionPredicateIsUsed() {
        addIndex(map, "this", false);

        for (int i = 0; i < 100; ++i) {
            map.put(i, i);
        }

        map.entrySet(Predicates.partitionPredicate(10, Predicates.equal("this", 10)));
        assertEquals(1, stats().getQueryCount());
        assertEquals(0, stats().getIndexedQueryCount());
        assertEquals(0, valueStats().getQueryCount());
    }

    @Test
    public void testQueryCounting_WhenStatisticsIsDisabled() {
        addIndex(map, "__key", false);
        addIndex(map, "this", true);

        addIndex(noStatsMap, "__key", false);
        addIndex(noStatsMap, "this", true);

        for (int i = 0; i < 100; ++i) {
            noStatsMap.put(i, i);
        }
        assertEquals(0, noStats().getQueryCount());
        assertEquals(0, stats().getQueryCount());

        noStatsMap.entrySet(Predicates.equal("__key", 10));
        assertEquals(0, noStats().getQueryCount());
        assertEquals(0, stats().getQueryCount());

        map.entrySet(Predicates.equal("__key", 10));
        assertEquals(0, noStats().getQueryCount());
        assertEquals(1, stats().getQueryCount());
    }

    @Test
    public void testMemoryCostTracking() {
        addIndex(map, "__key", false);
        addIndex(map, "this", true);
        long keyEmptyCost = keyStats().getMemoryCost();
        long valueEmptyCost = valueStats().getMemoryCost();
        assertTrue(keyEmptyCost > 0);
        assertTrue(valueEmptyCost > 0);

        for (int i = 0; i < 10000; ++i) {
            map.put(i, i);
        }
        long keyFullCost = keyStats().getMemoryCost();
        long valueFullCost = valueStats().getMemoryCost();
        assertTrue(keyFullCost > keyEmptyCost);
        assertTrue(valueFullCost > valueEmptyCost);

        for (int i = 0; i < 5000; ++i) {
            map.remove(i);
        }
        long keyHalfFullCost = keyStats().getMemoryCost();
        long valueHalfFullCost = valueStats().getMemoryCost();
        // keyHalfFullCost < keyFullCost does not necessarily hold, since
        // keys are inlined and b+ tree nodes are deleted only when they become
        // fully empty
        assertTrue(keyHalfFullCost > keyEmptyCost);
        assertTrue(valueHalfFullCost > valueEmptyCost && valueHalfFullCost < valueFullCost);

        // 'force' some extra pages to be allocated
        for (int i = 10000; i < 15000; ++i) {
            map.put(i, i);
        }
        assertTrue(keyStats().getMemoryCost() > keyHalfFullCost);
        assertTrue(valueStats().getMemoryCost() > valueHalfFullCost);

        for (int i = 0; i < 5000; ++i) {
            map.set(i, i * i);
        }
        assertTrue(keyStats().getMemoryCost() > keyHalfFullCost);
        assertTrue(valueStats().getMemoryCost() > valueHalfFullCost);
    }

    @Test
    public void testMemoryCostIsNotDrifting() {
        addIndex(map, "__key", false);
        addIndex(map, "this", true);

        long keyEmptyCost = keyStats().getMemoryCost();
        long valueEmptyCost = valueStats().getMemoryCost();
        assertTrue(keyEmptyCost > 0);
        assertTrue(valueEmptyCost > 0);

        map.set(0, 0);
        long keyPopulatedCost = keyStats().getMemoryCost();
        long valuePopulatedCost = valueStats().getMemoryCost();
        assertTrue(keyPopulatedCost > keyEmptyCost);
        assertTrue(valuePopulatedCost > valueEmptyCost);

        for (int i = 0; i < 100; ++i) {
            map.set(0, 0);
            assertEquals(keyPopulatedCost, keyStats().getMemoryCost());
            assertEquals(valuePopulatedCost, valueStats().getMemoryCost());

            map.remove(0);
            assertEquals(keyEmptyCost, keyStats().getMemoryCost());
            assertEquals(valueEmptyCost, valueStats().getMemoryCost());

            map.set(0, 0);
            assertEquals(keyPopulatedCost, keyStats().getMemoryCost());
            assertEquals(valuePopulatedCost, valueStats().getMemoryCost());
        }
    }

    @Test
    public void testAverageQueryLatencyTracking() {
        addIndex(map, "__key", false);
        assertEquals(0, keyStats().getAverageHitLatency());

        for (int i = 0; i < 100; ++i) {
            map.put(i, i);
        }
        assertEquals(0, keyStats().getAverageHitLatency());

        long totalMeasuredLatency = 0;
        for (int i = 1; i <= QUERIES; ++i) {
            long startNanos = Timer.nanos();
            map.entrySet(Predicates.equal("__key", i));
            totalMeasuredLatency += Timer.nanosElapsed(startNanos);

            assertTrue(keyStats().getAverageHitLatency() > 0);
            assertTrue(keyStats().getAverageHitLatency() <= totalMeasuredLatency / i);
        }

        long originalAvgHitLatency = keyStats().getAverageHitLatency();
        for (int i = 1; i <= QUERIES; ++i) {
            map.entrySet(Predicates.alwaysTrue());
        }
        long avgHitLatencyAfterNonIndexedQueries = keyStats().getAverageHitLatency();
        assertEquals(originalAvgHitLatency, avgHitLatencyAfterNonIndexedQueries);

    }

    @Test
    public void testInsertsTracking() {
        addIndex(map, "__key", false);
        assertEquals(0, keyStats().getInsertCount());
        assertEquals(0, keyStats().getTotalInsertLatency());

        long totalMeasuredLatency = 0;
        long previousTotalInsertLatency = 0;
        for (int i = 1; i <= 100; ++i) {
            long startNanos = Timer.nanos();
            map.put(i, i);
            totalMeasuredLatency += Timer.nanosElapsed(startNanos);

            assertEquals(i, keyStats().getInsertCount());
            assertTrue(keyStats().getTotalInsertLatency() > previousTotalInsertLatency);
            assertTrue(keyStats().getTotalInsertLatency() <= totalMeasuredLatency);

            previousTotalInsertLatency = keyStats().getTotalInsertLatency();
        }

        assertEquals(0, keyStats().getUpdateCount());
        assertEquals(0, keyStats().getRemoveCount());
    }

    @Test
    public void testUpdateTracking() {
        addIndex(map, "__key", false);
        assertEquals(0, keyStats().getUpdateCount());
        assertEquals(0, keyStats().getTotalUpdateLatency());

        for (int i = 1; i <= 100; ++i) {
            map.put(i, i);
        }
        assertEquals(0, keyStats().getUpdateCount());
        assertEquals(0, keyStats().getTotalUpdateLatency());

        long totalMeasuredLatency = 0;
        long previousTotalUpdateLatency = 0;
        for (int i = 1; i <= 50; ++i) {
            long startNanos = Timer.nanos();
            map.put(i, i * 2);
            totalMeasuredLatency += Timer.nanosElapsed(startNanos);

            assertEquals(i, keyStats().getUpdateCount());
            assertTrue(keyStats().getTotalUpdateLatency() > previousTotalUpdateLatency);
            assertTrue(keyStats().getTotalUpdateLatency() <= totalMeasuredLatency);

            previousTotalUpdateLatency = keyStats().getTotalUpdateLatency();
        }

        assertEquals(100, keyStats().getInsertCount());
        assertEquals(0, keyStats().getRemoveCount());
    }

    @Test
    public void testRemoveTracking() {
        addIndex(map, "__key", false);
        assertEquals(0, keyStats().getRemoveCount());
        assertEquals(0, keyStats().getTotalRemoveLatency());

        for (int i = 1; i <= 100; ++i) {
            map.put(i, i);
        }
        assertEquals(0, keyStats().getRemoveCount());
        assertEquals(0, keyStats().getTotalRemoveLatency());

        long totalMeasuredLatency = 0;
        long previousTotalRemoveLatency = 0;
        for (int i = 1; i <= 50; ++i) {
            long startNanos = Timer.nanos();
            map.remove(i);
            totalMeasuredLatency += Timer.nanosElapsed(startNanos);

            assertEquals(i, keyStats().getRemoveCount());
            assertTrue(keyStats().getTotalRemoveLatency() > previousTotalRemoveLatency);
            assertTrue(keyStats().getTotalRemoveLatency() <= totalMeasuredLatency);

            previousTotalRemoveLatency = keyStats().getTotalRemoveLatency();
        }

        assertEquals(100, keyStats().getInsertCount());
        assertEquals(0, keyStats().getUpdateCount());
    }

    @Test
    public void testInsertUpdateRemoveAreNotAffectingEachOther() {
        addIndex(map, "__key", false);
        assertEquals(0, keyStats().getInsertCount());
        assertEquals(0, keyStats().getUpdateCount());
        assertEquals(0, keyStats().getRemoveCount());

        long originalTotalInsertLatency = keyStats().getTotalInsertLatency();
        long originalTotalUpdateLatency = keyStats().getTotalUpdateLatency();
        long originalTotalRemoveLatency = keyStats().getTotalRemoveLatency();

        for (int i = 1; i <= 100; ++i) {
            map.put(i, i);
        }

        assertTrue(keyStats().getTotalInsertLatency() > originalTotalInsertLatency);
        assertEquals(originalTotalUpdateLatency, keyStats().getTotalUpdateLatency());
        assertEquals(originalTotalRemoveLatency, keyStats().getTotalRemoveLatency());

        originalTotalInsertLatency = keyStats().getTotalInsertLatency();
        originalTotalUpdateLatency = keyStats().getTotalUpdateLatency();
        originalTotalRemoveLatency = keyStats().getTotalRemoveLatency();

        for (int i = 1; i <= 50; ++i) {
            map.put(i, i * 2);
        }

        assertEquals(originalTotalInsertLatency, keyStats().getTotalInsertLatency());
        assertTrue(keyStats().getTotalUpdateLatency() > originalTotalUpdateLatency);
        assertEquals(originalTotalRemoveLatency, keyStats().getTotalRemoveLatency());

        originalTotalInsertLatency = keyStats().getTotalInsertLatency();
        originalTotalUpdateLatency = keyStats().getTotalUpdateLatency();
        originalTotalRemoveLatency = keyStats().getTotalRemoveLatency();

        for (int i = 1; i <= 20; ++i) {
            map.remove(i);
        }

        assertEquals(originalTotalInsertLatency, keyStats().getTotalInsertLatency());
        assertEquals(originalTotalUpdateLatency, keyStats().getTotalUpdateLatency());
        assertTrue(keyStats().getTotalRemoveLatency() > originalTotalRemoveLatency);

        assertEquals(100, keyStats().getInsertCount());
        assertEquals(50, keyStats().getUpdateCount());
        assertEquals(20, keyStats().getRemoveCount());

    }

    @Test
    public void testIndexStatsAfterMapDestroy() {
        addIndex(map, "this", true);
        for (int i = 0; i < 100; ++i) {
            map.put(i, i);
        }
        for (int i = 90; i < 100; ++i) {
            map.remove(i, i);
        }
        for (int i = 70; i < 90; ++i) {
            map.set(i, i * i);
        }

        map.entrySet(Predicates.equal("this", 10));
        assertEquals(1, stats().getQueryCount());
        assertEquals(1, stats().getIndexedQueryCount());
        assertEquals(1, valueStats().getQueryCount());
        assertEquals(100, valueStats().getInsertCount());
        assertEquals(20, valueStats().getUpdateCount());
        assertEquals(10, valueStats().getRemoveCount());
        assertTrue(valueStats().getTotalInsertLatency() > 0);
        assertTrue(valueStats().getTotalRemoveLatency() > 0);
        assertTrue(valueStats().getTotalUpdateLatency() > 0);
        assertTrue(valueStats().getAverageHitLatency() > 0);

        map.destroy();
        assertNull(valueStats());

        map = instance.getMap(mapName);
        assertNull(valueStats());
        addIndex(map, "this", true);
        assertNotNull(valueStats());

        assertEquals(0, stats().getQueryCount());
        assertEquals(0, stats().getIndexedQueryCount());
        assertEquals(0, valueStats().getQueryCount());
        assertEquals(0, valueStats().getInsertCount());
        assertEquals(0, valueStats().getUpdateCount());
        assertEquals(0, valueStats().getRemoveCount());
        assertEquals(0, valueStats().getTotalInsertLatency());
        assertEquals(0, valueStats().getTotalRemoveLatency());
        assertEquals(0, valueStats().getTotalUpdateLatency());
        assertEquals(0, valueStats().getAverageHitLatency());

        for (int i = 0; i < 50; ++i) {
            map.put(i, i);
        }
        for (int i = 45; i < 50; ++i) {
            map.remove(i, i);
        }
        for (int i = 35; i < 45; ++i) {
            map.set(i, i * i);
        }

        map.entrySet(Predicates.equal("this", 10));
        assertEquals(1, stats().getQueryCount());
        assertEquals(1, stats().getIndexedQueryCount());
        assertEquals(1, valueStats().getQueryCount());
        assertEquals(50, valueStats().getInsertCount());
        assertEquals(10, valueStats().getUpdateCount());
        assertEquals(5, valueStats().getRemoveCount());
        assertTrue(valueStats().getTotalInsertLatency() > 0);
        assertTrue(valueStats().getTotalRemoveLatency() > 0);
        assertTrue(valueStats().getTotalUpdateLatency() > 0);
        assertTrue(valueStats().getAverageHitLatency() > 0);

    }

    protected LocalMapStats stats() {
        return map.getLocalMapStats();
    }

    protected LocalMapStats noStats() {
        return noStatsMap.getLocalMapStats();
    }

    protected LocalIndexStats keyStats() {
        return stats().getIndexStats().get("__key");
    }

    protected LocalIndexStats valueStats() {
        return stats().getIndexStats().get("this");
    }

    private void query(QueryType queryType, Predicate... predicates) {
        for (int i = 0; i < QUERIES; ++i) {
            for (Predicate predicate : predicates) {
                queryType.query(predicate);
            }
        }
    }

    protected static void addIndex(IMap map, String attribute, boolean ordered) {
        IndexConfig config = new IndexConfig(ordered ? IndexType.SORTED : IndexType.HASH, attribute).setName(attribute);

        map.addIndex(config);
    }

    private interface QueryType {

        boolean isIndexed();

        void query(Predicate predicate);

    }

    @SuppressWarnings("unchecked")
    private QueryType[] initQueryTypes() {
        final IMap map = this.map;

        return new QueryType[]{new QueryType() {
            @Override
            public boolean isIndexed() {
                return false;
            }

            @Override
            public void query(Predicate predicate) {
                map.entrySet();
            }
        }, new QueryType() {
            @Override
            public boolean isIndexed() {
                return true;
            }

            @Override
            public void query(Predicate predicate) {
                map.entrySet(predicate);
            }
        }, new QueryType() {
            @Override
            public boolean isIndexed() {
                return false;
            }

            @Override
            public void query(Predicate predicate) {
                map.values();
            }
        }, new QueryType() {
            @Override
            public boolean isIndexed() {
                return true;
            }

            @Override
            public void query(Predicate predicate) {
                map.values(predicate);
            }
        }, new QueryType() {
            @Override
            public boolean isIndexed() {
                return false;
            }

            @Override
            public void query(Predicate predicate) {
                map.keySet();
            }
        }, new QueryType() {
            @Override
            public boolean isIndexed() {
                return true;
            }

            @Override
            public void query(Predicate predicate) {
                map.keySet(predicate);
            }
        }, new QueryType() {
            @Override
            public boolean isIndexed() {
                return false;
            }

            @Override
            public void query(Predicate predicate) {
                map.aggregate(Aggregators.count());
            }
        }, new QueryType() {
            @Override
            public boolean isIndexed() {
                return true;
            }

            @Override
            public void query(Predicate predicate) {
                map.aggregate(Aggregators.count(), predicate);
            }
        }, new QueryType() {
            @Override
            public boolean isIndexed() {
                return false;
            }

            @Override
            public void query(Predicate predicate) {
                map.project(Projections.singleAttribute("this"));
            }
        }, new QueryType() {
            @Override
            public boolean isIndexed() {
                return true;
            }

            @Override
            public void query(Predicate predicate) {
                map.project(Projections.singleAttribute("this"), predicate);
            }
        }
        };
    }
}
