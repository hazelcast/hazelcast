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

package com.hazelcast.map;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.LocalIndexStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class LocalIndexStatsTest extends HazelcastTestSupport {

    @Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.OBJECT}, {InMemoryFormat.BINARY}});
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    private static final int PARTITIONS = 137;

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
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), Integer.toString(PARTITIONS));
        config.getMapConfig(mapName).setInMemoryFormat(inMemoryFormat);
        config.getMapConfig(noStatsMapName).setStatisticsEnabled(false);

        instance = createInstance(config);
        map = instance.getMap(mapName);
        noStatsMap = instance.getMap(noStatsMapName);
        queryTypes = initQueryTypes();
    }

    protected HazelcastInstance createInstance(Config config) {
        return createHazelcastInstance(config);
    }

    @Test
    public void testQueryCounting() {
        map.addIndex("this", false);
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
        map.addIndex("__key", false);
        map.addIndex("this", true);
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
        map.addIndex("__key", false);
        map.addIndex("this", true);
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
        map.addIndex("__key", false);
        map.addIndex("this", true);
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

        map.addIndex("__key", false);
        map.addIndex("this", true);
        for (int i = 0; i < entryCount; ++i) {
            map.put(i, i);
        }
        assertEquals(0.0, keyStats().getAverageHitSelectivity(), 0.0);
        assertEquals(0.0, valueStats().getAverageHitSelectivity(), 0.0);

        for (int i = 0; i < QUERIES; ++i) {
            map.entrySet(Predicates.equal("__key", 10));
            map.entrySet(Predicates.equal("this", 10));
            assertEquals(expected, keyStats().getAverageHitSelectivity(), 0.01);
            assertEquals(expected, valueStats().getAverageHitSelectivity(), 0.01);
        }

        for (int i = 1; i <= QUERIES; ++i) {
            map.entrySet(Predicates.greaterEqual("__key", entryCount / 2));
            map.entrySet(Predicates.greaterEqual("this", entryCount / 2));
            assertEquals((expected * QUERIES + 0.5 * i) / (QUERIES + i), keyStats().getAverageHitSelectivity(), 0.01);
            assertEquals((expected * QUERIES + 0.5 * i) / (QUERIES + i), valueStats().getAverageHitSelectivity(), 0.01);
        }
    }

    @Test
    public void testQueryCounting_WhenTwoMapsUseIndexesNamedTheSame() {
        map.addIndex("__key", false);
        map.addIndex("this", true);

        IMap<Integer, Integer> otherMap = instance.getMap(map.getName() + "_other_map");
        otherMap.addIndex("__key", false);
        otherMap.addIndex("this", true);

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

    @SuppressWarnings("unchecked")
    @Test
    public void testQueryCounting_WhenPartitionPredicateIsUsed() {
        map.addIndex("this", false);

        for (int i = 0; i < 100; ++i) {
            map.put(i, i);
        }

        map.entrySet(new PartitionPredicate(10, Predicates.equal("this", 10)));
        assertEquals(1, stats().getQueryCount());
        assertEquals(0, stats().getIndexedQueryCount());
        assertEquals(0, valueStats().getQueryCount());
    }

    @Test
    public void testQueryCounting_WhenStatisticsIsDisabled() {
        map.addIndex("__key", false);
        map.addIndex("this", true);

        noStatsMap.addIndex("__key", false);
        noStatsMap.addIndex("this", true);

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
        map.addIndex("__key", false);
        map.addIndex("this", true);
        long keyEmptyCost = keyStats().getOnHeapMemoryCost();
        long valueEmptyCost = valueStats().getOnHeapMemoryCost();
        assertTrue(keyEmptyCost > 0);
        assertTrue(valueEmptyCost > 0);
        assertEquals(0, keyStats().getOffHeapMemoryCost());
        assertEquals(0, valueStats().getOffHeapMemoryCost());

        for (int i = 0; i < 100; ++i) {
            map.put(i, i);
        }
        long keyFullCost = keyStats().getOnHeapMemoryCost();
        long valueFullCost = valueStats().getOnHeapMemoryCost();
        assertTrue(keyFullCost > keyEmptyCost);
        assertTrue(valueFullCost > valueEmptyCost);
        assertEquals(0, keyStats().getOffHeapMemoryCost());
        assertEquals(0, valueStats().getOffHeapMemoryCost());

        for (int i = 0; i < 50; ++i) {
            map.remove(i);
        }
        long keyHalfFullCost = keyStats().getOnHeapMemoryCost();
        long valueHalfFullCost = valueStats().getOnHeapMemoryCost();
        assertTrue(keyHalfFullCost > keyEmptyCost && keyHalfFullCost < keyFullCost);
        assertTrue(valueHalfFullCost > valueEmptyCost && valueHalfFullCost < valueFullCost);
        assertEquals(0, keyStats().getOffHeapMemoryCost());
        assertEquals(0, valueStats().getOffHeapMemoryCost());

        for (int i = 0; i < 50; ++i) {
            map.put(i, i);
        }
        assertTrue(keyStats().getOnHeapMemoryCost() > keyHalfFullCost);
        assertTrue(valueStats().getOnHeapMemoryCost() > valueHalfFullCost);
        assertEquals(0, keyStats().getOffHeapMemoryCost());
        assertEquals(0, valueStats().getOffHeapMemoryCost());

        for (int i = 0; i < 50; ++i) {
            map.set(i, i * i);
        }
        assertTrue(keyStats().getOnHeapMemoryCost() > keyHalfFullCost);
        assertTrue(valueStats().getOnHeapMemoryCost() > valueHalfFullCost);
        assertEquals(0, keyStats().getOffHeapMemoryCost());
        assertEquals(0, valueStats().getOffHeapMemoryCost());
    }

    @Test
    public void testAverageQueryLatencyTracking() {
        map.addIndex("__key", false);
        assertEquals(0, keyStats().getAverageHitLatency());

        for (int i = 0; i < 100; ++i) {
            map.put(i, i);
        }
        assertEquals(0, keyStats().getAverageHitLatency());

        long totalMeasuredLatency = 0;
        for (int i = 1; i <= QUERIES; ++i) {
            long start = System.nanoTime();
            map.entrySet(Predicates.equal("__key", i));
            totalMeasuredLatency += System.nanoTime() - start;

            assertTrue(keyStats().getAverageHitLatency() > 0);
            assertTrue(keyStats().getAverageHitLatency() <= totalMeasuredLatency / i);
        }
    }

    @Test
    public void testInsertsTracking() {
        map.addIndex("__key", false);
        assertEquals(0, keyStats().getInsertCount());
        assertEquals(0, keyStats().getTotalInsertLatency());

        long totalMeasuredLatency = 0;
        for (int i = 1; i <= 100; ++i) {
            long start = System.nanoTime();
            map.put(i, i);
            totalMeasuredLatency += System.nanoTime() - start;

            assertEquals(i, keyStats().getInsertCount());
            assertTrue(keyStats().getTotalInsertLatency() > 0);
            assertTrue(keyStats().getTotalInsertLatency() <= totalMeasuredLatency);
        }

        assertEquals(0, keyStats().getUpdateCount());
        assertEquals(0, keyStats().getRemoveCount());
    }

    @Test
    public void testUpdateTracking() {
        map.addIndex("__key", false);
        assertEquals(0, keyStats().getUpdateCount());
        assertEquals(0, keyStats().getTotalUpdateLatency());

        for (int i = 1; i <= 100; ++i) {
            map.put(i, i);
        }
        assertEquals(0, keyStats().getUpdateCount());
        assertEquals(0, keyStats().getTotalUpdateLatency());

        long totalMeasuredLatency = 0;
        for (int i = 1; i <= 50; ++i) {
            long start = System.nanoTime();
            map.put(i, i * 2);
            totalMeasuredLatency += System.nanoTime() - start;

            assertEquals(i, keyStats().getUpdateCount());
            assertTrue(keyStats().getTotalUpdateLatency() > 0);
            assertTrue(keyStats().getTotalUpdateLatency() <= totalMeasuredLatency);
        }

        assertEquals(100, keyStats().getInsertCount());
        assertEquals(0, keyStats().getRemoveCount());
    }

    @Test
    public void testRemoveTracking() {
        map.addIndex("__key", false);
        assertEquals(0, keyStats().getRemoveCount());
        assertEquals(0, keyStats().getTotalRemoveLatency());

        for (int i = 1; i <= 100; ++i) {
            map.put(i, i);
        }
        assertEquals(0, keyStats().getRemoveCount());
        assertEquals(0, keyStats().getTotalRemoveLatency());

        long totalMeasuredLatency = 0;
        for (int i = 1; i <= 50; ++i) {
            long start = System.nanoTime();
            map.remove(i);
            totalMeasuredLatency += System.nanoTime() - start;

            assertEquals(i, keyStats().getRemoveCount());
            assertTrue(keyStats().getTotalRemoveLatency() > 0);
            assertTrue(keyStats().getTotalRemoveLatency() <= totalMeasuredLatency);
        }

        assertEquals(100, keyStats().getInsertCount());
        assertEquals(0, keyStats().getUpdateCount());
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
        }, };
    }

}
