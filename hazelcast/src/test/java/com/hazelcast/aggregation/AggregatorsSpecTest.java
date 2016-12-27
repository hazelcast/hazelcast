package com.hazelcast.aggregation;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.projection.Projections;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import static com.hazelcast.spi.properties.GroupProperty.AGGREGATION_ACCUMULATION_PARALLEL_EVALUATION;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AggregatorsSpecTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @After
    public void tearDown() {
        if (factory != null) {
            factory.terminateAll();
        }
    }

    @Test
    public void testAggregators_withParallelAccumulation() {
        IMap<Integer, Person> map = getMapWithNodeCount(3, true);
        populateMapWithPersons(map, 1000);

        assertMinAggregators(map);
        assertMaxAggregators(map);
        assertSumAggregators(map);
        assertAverageAggregators(map);
        assertCountAggregators(map);
        assertDistinctAggregators(map);
    }

    @Test
    public void testAggregators_withCallerRunsAccumulation() {
        IMap<Integer, Person> map = getMapWithNodeCount(2, false);
        populateMapWithPersons(map, 1000);

        assertMinAggregators(map);
        assertMaxAggregators(map);
        assertSumAggregators(map);
        assertAverageAggregators(map);
        assertCountAggregators(map);
        assertDistinctAggregators(map);
    }

    private <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount, boolean parallelAccumulation) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count < 1");
        }

        factory = createHazelcastInstanceFactory(nodeCount);

        MapConfig mapConfig = new MapConfig()
                .setName("aggr")
                .setInMemoryFormat(InMemoryFormat.OBJECT);

        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), String.valueOf(nodeCount))
                .setProperty(AGGREGATION_ACCUMULATION_PARALLEL_EVALUATION.getName(), String.valueOf(parallelAccumulation))
                .addMapConfig(mapConfig);

        HazelcastInstance instance = factory.newInstances(config)[0];
        return instance.getMap("aggr");
    }

    private static void assertMinAggregators(IMap<Integer, Person> map) {
        assertEquals(Double.valueOf(1), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>doubleMin("doubleValue")));
        assertEquals(Long.valueOf(1), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>longMin("longValue")));
        assertEquals(Integer.valueOf(1), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>integerMin("intValue")));
        assertEquals(BigDecimal.valueOf(1), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>bigDecimalMin("bigDecimalValue")));
        assertEquals(BigInteger.valueOf(1), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>bigIntegerMin("bigIntegerValue")));

        assertEquals(Double.valueOf(1), map.aggregate(Aggregators.<Map.Entry<Integer, Person>, Double>comparableMin("doubleValue")));
        assertEquals(Long.valueOf(1), map.aggregate(Aggregators.<Map.Entry<Integer, Person>, Long>comparableMin("longValue")));
        assertEquals(Integer.valueOf(1), map.aggregate(Aggregators.<Map.Entry<Integer, Person>, Integer>comparableMin("intValue")));
        assertEquals(BigDecimal.valueOf(1), map.aggregate(Aggregators.<Map.Entry<Integer, Person>, BigDecimal>comparableMin("bigDecimalValue")));
        assertEquals(BigInteger.valueOf(1), map.aggregate(Aggregators.<Map.Entry<Integer, Person>, BigInteger>comparableMin("bigIntegerValue")));

        assertEquals("1", map.aggregate(Aggregators.<Map.Entry<Integer, Person>, String>comparableMin("comparableValue")));
    }

    private static void assertMaxAggregators(IMap<Integer, Person> map) {
        assertEquals(Double.valueOf(999), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>doubleMax("doubleValue")));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>longMax("longValue")));
        assertEquals(Integer.valueOf(999), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>integerMax("intValue")));
        assertEquals(BigDecimal.valueOf(999), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>bigDecimalMax("bigDecimalValue")));
        assertEquals(BigInteger.valueOf(999), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>bigIntegerMax("bigIntegerValue")));

        assertEquals(Double.valueOf(999), map.aggregate(Aggregators.<Map.Entry<Integer, Person>, Double>comparableMax("doubleValue")));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.<Map.Entry<Integer, Person>, Long>comparableMax("longValue")));
        assertEquals(Integer.valueOf(999), map.aggregate(Aggregators.<Map.Entry<Integer, Person>, Integer>comparableMax("intValue")));
        assertEquals(BigDecimal.valueOf(999), map.aggregate(Aggregators.<Map.Entry<Integer, Person>, BigDecimal>comparableMax("bigDecimalValue")));
        assertEquals(BigInteger.valueOf(999), map.aggregate(Aggregators.<Map.Entry<Integer, Person>, BigInteger>comparableMax("bigIntegerValue")));

        assertEquals("999", map.aggregate(Aggregators.<Map.Entry<Integer, Person>, String>comparableMax("comparableValue")));
    }

    private static void assertSumAggregators(IMap<Integer, Person> map) {
        assertEquals(Double.valueOf(499500.0d), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>doubleSum("doubleValue")));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>longSum("longValue")));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>integerSum("intValue")));
        assertEquals(BigDecimal.valueOf(499500), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>bigDecimalSum("bigDecimalValue")));
        assertEquals(BigInteger.valueOf(499500), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>bigIntegerSum("bigIntegerValue")));

        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>fixedPointSum("doubleValue")));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>fixedPointSum("longValue")));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>fixedPointSum("intValue")));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>fixedPointSum("bigIntegerValue")));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>fixedPointSum("bigDecimalValue")));

        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>floatingPointSum("doubleValue")));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>floatingPointSum("longValue")));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>floatingPointSum("intValue")));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>floatingPointSum("bigIntegerValue")));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>floatingPointSum("bigDecimalValue")));
    }

    private static void assertAverageAggregators(IMap<Integer, Person> map) {
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>doubleAvg("doubleValue")));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>longAvg("longValue")));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>integerAvg("intValue")));
        assertEquals(BigDecimal.valueOf(500), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>bigDecimalAvg("bigDecimalValue")));
        assertEquals(BigDecimal.valueOf(500), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>bigIntegerAvg("bigIntegerValue")));

        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>numberAvg("doubleValue")));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>numberAvg("longValue")));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>numberAvg("intValue")));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>numberAvg("bigDecimalValue")));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>numberAvg("bigIntegerValue")));
    }

    private static void assertCountAggregators(IMap<Integer, Person> map) {
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>count("doubleValue")));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>count("longValue")));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>count("intValue")));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>count("bigDecimalValue")));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>count("bigIntegerValue")));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.<Map.Entry<Integer, Person>>count("comparableValue")));
    }

    private static void assertDistinctAggregators(IMap<Integer, Person> map) {
        assertEquals(map.project(Projections.<Map.Entry<Integer, Person>, Double>singleAttribute("doubleValue")),
                map.aggregate(Aggregators.<Map.Entry<Integer, Person>, Double>distinct("doubleValue")));
        assertEquals(map.project(Projections.<Map.Entry<Integer, Person>, Long>singleAttribute("longValue")),
                map.aggregate(Aggregators.<Map.Entry<Integer, Person>, Long>distinct("longValue")));
        assertEquals(map.project(Projections.<Map.Entry<Integer, Person>, Integer>singleAttribute("intValue")),
                map.aggregate(Aggregators.<Map.Entry<Integer, Person>, Integer>distinct("intValue")));
        assertEquals(map.project(Projections.<Map.Entry<Integer, Person>, BigDecimal>singleAttribute("bigDecimalValue")),
                map.aggregate(Aggregators.<Map.Entry<Integer, Person>, BigDecimal>distinct("bigDecimalValue")));
        assertEquals(map.project(Projections.<Map.Entry<Integer, Person>, BigInteger>singleAttribute("bigIntegerValue")),
                map.aggregate(Aggregators.<Map.Entry<Integer, Person>, BigInteger>distinct("bigIntegerValue")));
        assertEquals(map.project(Projections.<Map.Entry<Integer, Person>, Comparable>singleAttribute("comparableValue")),
                map.aggregate(Aggregators.<Map.Entry<Integer, Person>, Comparable>distinct("comparableValue")));
    }

    private static void populateMapWithPersons(IMap<Integer, Person> map, int count) {
        for (int i = 1; i < count; i++) {
            map.put(i, new Person(i));
        }
    }

    public static class Person implements Serializable {

        public int intValue;
        public double doubleValue;
        public long longValue;
        public BigDecimal bigDecimalValue;
        public BigInteger bigIntegerValue;
        public String comparableValue;

        public Person() {
        }

        public Person(int numberValue) {
            this.intValue = numberValue;
            this.doubleValue = numberValue;
            this.longValue = numberValue;
            this.bigDecimalValue = BigDecimal.valueOf(numberValue);
            this.bigIntegerValue = BigInteger.valueOf(numberValue);
            this.comparableValue = String.valueOf(numberValue);
        }
    }
}
