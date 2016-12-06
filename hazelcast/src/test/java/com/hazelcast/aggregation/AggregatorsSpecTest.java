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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AggregatorsSpecTest extends HazelcastTestSupport {

    @Test
    public void average_3Nodes() {
        IMap<Integer, Person> map = getMapWithNodeCount(3);
        populateMapWithPersons(map, 1000);

        assertMinAggregators(map);
        assertMaxAggregators(map);
        assertSumAggregators(map);
        assertAverageAggregators(map);
        assertCountAggregators(map);
        assertDistinctAggregators(map);
    }

    private void assertMinAggregators(IMap<Integer, Person> map) {
        assertEquals(Double.valueOf(1), map.aggregate(Aggregators.<Integer, Person>doubleMin("doubleValue")));
        assertEquals(Long.valueOf(1), map.aggregate(Aggregators.<Integer, Person>longMin("longValue")));
        assertEquals(Integer.valueOf(1), map.aggregate(Aggregators.<Integer, Person>integerMin("intValue")));
        assertEquals(BigDecimal.valueOf(1), map.aggregate(Aggregators.<Integer, Person>bigDecimalMin("bigDecimalValue")));
        assertEquals(BigInteger.valueOf(1), map.aggregate(Aggregators.<Integer, Person>bigIntegerMin("bigIntegerValue")));

        assertEquals(Double.valueOf(1), map.aggregate(Aggregators.<Double, Integer, Person>comparableMin("doubleValue")));
        assertEquals(Long.valueOf(1), map.aggregate(Aggregators.<Long, Integer, Person>comparableMin("longValue")));
        assertEquals(Integer.valueOf(1), map.aggregate(Aggregators.<Integer, Integer, Person>comparableMin("intValue")));
        assertEquals(BigDecimal.valueOf(1), map.aggregate(Aggregators.<BigDecimal, Integer, Person>comparableMin("bigDecimalValue")));
        assertEquals(BigInteger.valueOf(1), map.aggregate(Aggregators.<BigInteger, Integer, Person>comparableMin("bigIntegerValue")));

        assertEquals("1", map.aggregate(Aggregators.<String, Integer, Person>comparableMin("comparableValue")));

    }

    private void assertMaxAggregators(IMap<Integer, Person> map) {
        assertEquals(Double.valueOf(999), map.aggregate(Aggregators.<Integer, Person>doubleMax("doubleValue")));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.<Integer, Person>longMax("longValue")));
        assertEquals(Integer.valueOf(999), map.aggregate(Aggregators.<Integer, Person>integerMax("intValue")));
        assertEquals(BigDecimal.valueOf(999), map.aggregate(Aggregators.<Integer, Person>bigDecimalMax("bigDecimalValue")));
        assertEquals(BigInteger.valueOf(999), map.aggregate(Aggregators.<Integer, Person>bigIntegerMax("bigIntegerValue")));

        assertEquals(Double.valueOf(999), map.aggregate(Aggregators.<Double, Integer, Person>comparableMax("doubleValue")));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.<Long, Integer, Person>comparableMax("longValue")));
        assertEquals(Integer.valueOf(999), map.aggregate(Aggregators.<Integer, Integer, Person>comparableMax("intValue")));
        assertEquals(BigDecimal.valueOf(999), map.aggregate(Aggregators.<BigDecimal, Integer, Person>comparableMax("bigDecimalValue")));
        assertEquals(BigInteger.valueOf(999), map.aggregate(Aggregators.<BigInteger, Integer, Person>comparableMax("bigIntegerValue")));

        assertEquals("999", map.aggregate(Aggregators.<String, Integer, Person>comparableMax("comparableValue")));
    }

    private void assertSumAggregators(IMap<Integer, Person> map) {
        assertEquals(Double.valueOf(499500.0d), map.aggregate(Aggregators.<Integer, Person>doubleSum("doubleValue")));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.<Integer, Person>longSum("longValue")));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.<Integer, Person>integerSum("intValue")));
        assertEquals(BigDecimal.valueOf(499500), map.aggregate(Aggregators.<Integer, Person>bigDecimalSum("bigDecimalValue")));
        assertEquals(BigInteger.valueOf(499500), map.aggregate(Aggregators.<Integer, Person>bigIntegerSum("bigIntegerValue")));

        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.<Integer, Person>fixedPointSum("doubleValue")));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.<Integer, Person>fixedPointSum("longValue")));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.<Integer, Person>fixedPointSum("intValue")));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.<Integer, Person>fixedPointSum("bigIntegerValue")));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.<Integer, Person>fixedPointSum("bigDecimalValue")));

        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.<Integer, Person>floatingPointSum("doubleValue")));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.<Integer, Person>floatingPointSum("longValue")));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.<Integer, Person>floatingPointSum("intValue")));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.<Integer, Person>floatingPointSum("bigIntegerValue")));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.<Integer, Person>floatingPointSum("bigDecimalValue")));
    }

    private void assertAverageAggregators(IMap<Integer, Person> map) {
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.<Integer, Person>doubleAvg("doubleValue")));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.<Integer, Person>longAvg("longValue")));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.<Integer, Person>integerAvg("intValue")));
        assertEquals(BigDecimal.valueOf(500), map.aggregate(Aggregators.<Integer, Person>bigDecimalAvg("bigDecimalValue")));
        assertEquals(BigDecimal.valueOf(500), map.aggregate(Aggregators.<Integer, Person>bigIntegerAvg("bigIntegerValue")));

        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.<Integer, Person>numberAvg("doubleValue")));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.<Integer, Person>numberAvg("longValue")));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.<Integer, Person>numberAvg("intValue")));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.<Integer, Person>numberAvg("bigDecimalValue")));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.<Integer, Person>numberAvg("bigIntegerValue")));
    }

    private void assertCountAggregators(IMap<Integer, Person> map) {
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.<Integer, Person>count("doubleValue")));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.<Integer, Person>count("longValue")));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.<Integer, Person>count("intValue")));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.<Integer, Person>count("bigDecimalValue")));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.<Integer, Person>count("bigIntegerValue")));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.<Integer, Person>count("comparableValue")));
    }

    private void assertDistinctAggregators(IMap<Integer, Person> map) {
        assertEquals(map.project(Projections.<Integer, Person, Double>singleAttribute("doubleValue")),
                map.aggregate(Aggregators.<Double, Integer, Person>distinct("doubleValue")));
        assertEquals(map.project(Projections.<Integer, Person, Long>singleAttribute("longValue")),
                map.aggregate(Aggregators.<Long, Integer, Person>distinct("longValue")));
        assertEquals(map.project(Projections.<Integer, Person, Integer>singleAttribute("intValue")),
                map.aggregate(Aggregators.<Integer, Integer, Person>distinct("intValue")));
        assertEquals(map.project(Projections.<Integer, Person, BigDecimal>singleAttribute("bigDecimalValue")),
                map.aggregate(Aggregators.<BigDecimal, Integer, Person>distinct("bigDecimalValue")));
        assertEquals(map.project(Projections.<Integer, Person, BigInteger>singleAttribute("bigIntegerValue")),
                map.aggregate(Aggregators.<BigInteger, Integer, Person>distinct("bigIntegerValue")));
        assertEquals(map.project(Projections.<Integer, Person, Comparable>singleAttribute("comparableValue")),
                map.aggregate(Aggregators.<Comparable, Integer, Person>distinct("comparableValue")));
    }

    private static IMap<Integer, Person> populateMapWithPersons(IMap<Integer, Person> map, int count) {
        for (int i = 1; i < count; i++) {
            map.put(i, new Person(i));
        }
        return map;
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

    public <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count < 1");
        }

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);

        Config config = new Config();
        config.setProperty("hazelcast.partition.count", "3");
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("aggr");
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        config.addMapConfig(mapConfig);

        HazelcastInstance instance = factory.newInstances(config)[0];
        return instance.getMap("aggr");
    }

}
