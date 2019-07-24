/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.aggregation;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.projection.Projections;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static com.hazelcast.spi.properties.GroupProperty.AGGREGATION_ACCUMULATION_PARALLEL_EVALUATION;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AggregatorsSpecTest extends HazelcastTestSupport {

    public static final int PERSONS_COUNT = 999;

    @SuppressWarnings("DefaultAnnotationParam")
    @Parameter(0)
    public InMemoryFormat inMemoryFormat;

    @Parameter(1)
    public boolean parallelAccumulation;

    @Parameter(2)
    public String postfix;

    @Parameters(name = "{0} parallelAccumulation={1}, postfix={2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, false, ""},
                {InMemoryFormat.OBJECT, false, ""},
                {InMemoryFormat.BINARY, true, ""},
                {InMemoryFormat.OBJECT, true, ""},

                {InMemoryFormat.BINARY, false, "[any]"},
                {InMemoryFormat.OBJECT, false, "[any]"},
                {InMemoryFormat.BINARY, true, "[any]"},
                {InMemoryFormat.OBJECT, true, "[any]"},
        });
    }

    @Test
    public void testAggregators() {
        IMap<Integer, Person> map = getMapWithNodeCount(3, parallelAccumulation);
        populateMapWithPersons(map, postfix, PERSONS_COUNT);

        assertMinAggregators(map, postfix);
        assertMaxAggregators(map, postfix);
        assertSumAggregators(map, postfix);
        assertAverageAggregators(map, postfix);
        assertCountAggregators(map, postfix);
        assertDistinctAggregators(map, postfix);
    }

    public static void assertMinAggregators(IMap<Integer, Person> map, String p) {
        assertNoDataMissing(map, PERSONS_COUNT);
        assertEquals(Double.valueOf(1), map.aggregate(Aggregators.doubleMin("doubleValue" + p)));
        assertEquals(Long.valueOf(1), map.aggregate(Aggregators.longMin("longValue" + p)));
        assertEquals(Integer.valueOf(1), map.aggregate(Aggregators.integerMin("intValue" + p)));
        assertEquals(Integer.valueOf(1), map.aggregate(Aggregators.integerMin("optionalIntValue" + p)));
        assertEquals(BigDecimal.valueOf(1), map.aggregate(Aggregators.bigDecimalMin("bigDecimalValue" + p)));
        assertEquals(BigInteger.valueOf(1), map.aggregate(Aggregators.bigIntegerMin("bigIntegerValue" + p)));

        assertEquals(Double.valueOf(1), map.aggregate(Aggregators.comparableMin("doubleValue" + p)));
        assertEquals(Long.valueOf(1), map.aggregate(Aggregators.comparableMin("longValue" + p)));
        assertEquals(Integer.valueOf(1), map.aggregate(Aggregators.comparableMin("intValue" + p)));
        assertEquals(Integer.valueOf(1), map.aggregate(Aggregators.comparableMin("optionalIntValue" + p)));
        assertEquals(BigDecimal.valueOf(1), map.aggregate(Aggregators.comparableMin("bigDecimalValue" + p)));
        assertEquals(BigInteger.valueOf(1), map.aggregate(Aggregators.comparableMin("bigIntegerValue" + p)));

        assertEquals("1", map.aggregate(Aggregators.comparableMin("comparableValue" + p)));
        assertEquals("1", map.aggregate(Aggregators.comparableMin("optionalComparableValue" + p)));
    }

    public static void assertMaxAggregators(IMap<Integer, Person> map, String p) {
        assertNoDataMissing(map, PERSONS_COUNT);
        assertEquals(Double.valueOf(999), map.aggregate(Aggregators.doubleMax("doubleValue" + p)));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.longMax("longValue" + p)));
        assertEquals(Integer.valueOf(999), map.aggregate(Aggregators.integerMax("intValue" + p)));
        assertEquals(Integer.valueOf(999), map.aggregate(Aggregators.integerMax("optionalIntValue" + p)));
        assertEquals(BigDecimal.valueOf(999), map.aggregate(Aggregators.bigDecimalMax("bigDecimalValue" + p)));
        assertEquals(BigInteger.valueOf(999), map.aggregate(Aggregators.bigIntegerMax("bigIntegerValue" + p)));

        assertEquals(Double.valueOf(999), map.aggregate(Aggregators.comparableMax("doubleValue" + p)));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.comparableMax("longValue" + p)));
        assertEquals(Integer.valueOf(999), map.aggregate(Aggregators.comparableMax("intValue" + p)));
        assertEquals(Integer.valueOf(999), map.aggregate(Aggregators.comparableMax("optionalIntValue" + p)));
        assertEquals(BigDecimal.valueOf(999), map.aggregate(Aggregators.comparableMax("bigDecimalValue" + p)));
        assertEquals(BigInteger.valueOf(999), map.aggregate(Aggregators.comparableMax("bigIntegerValue" + p)));

        assertEquals("999", map.aggregate(Aggregators.comparableMax("comparableValue" + p)));
        assertEquals("999", map.aggregate(Aggregators.comparableMax("optionalComparableValue" + p)));
    }

    public static void assertSumAggregators(IMap<Integer, Person> map, String p) {
        assertNoDataMissing(map, PERSONS_COUNT);
        assertEquals(Double.valueOf(499500.0d), map.aggregate(Aggregators.doubleSum("doubleValue" + p)));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.longSum("longValue" + p)));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.integerSum("intValue" + p)));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.integerSum("optionalIntValue" + p)));
        assertEquals(BigDecimal.valueOf(499500), map.aggregate(Aggregators.bigDecimalSum("bigDecimalValue" + p)));
        assertEquals(BigInteger.valueOf(499500), map.aggregate(Aggregators.bigIntegerSum("bigIntegerValue" + p)));

        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.fixedPointSum("doubleValue" + p)));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.fixedPointSum("longValue" + p)));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.fixedPointSum("intValue" + p)));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.fixedPointSum("optionalIntValue" + p)));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.fixedPointSum("bigIntegerValue" + p)));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.fixedPointSum("bigDecimalValue" + p)));

        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.floatingPointSum("doubleValue" + p)));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.floatingPointSum("longValue" + p)));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.floatingPointSum("intValue" + p)));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.floatingPointSum("optionalIntValue" + p)));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.floatingPointSum("bigIntegerValue" + p)));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.floatingPointSum("bigDecimalValue" + p)));
    }

    public static void assertAverageAggregators(IMap<Integer, Person> map, String p) {
        assertNoDataMissing(map, PERSONS_COUNT);
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.doubleAvg("doubleValue" + p)));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.longAvg("longValue" + p)));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.integerAvg("intValue" + p)));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.integerAvg("optionalIntValue" + p)));
        assertEquals(BigDecimal.valueOf(500), map.aggregate(Aggregators.bigDecimalAvg("bigDecimalValue" + p)));
        assertEquals(BigDecimal.valueOf(500), map.aggregate(Aggregators.bigIntegerAvg("bigIntegerValue" + p)));

        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.numberAvg("doubleValue" + p)));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.numberAvg("longValue" + p)));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.numberAvg("intValue" + p)));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.numberAvg("optionalIntValue" + p)));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.numberAvg("bigDecimalValue" + p)));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.numberAvg("bigIntegerValue" + p)));
    }

    public static void assertCountAggregators(IMap<Integer, Person> map, String p) {
        assertNoDataMissing(map, PERSONS_COUNT);
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.count("doubleValue" + p)));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.count("longValue" + p)));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.count("intValue" + p)));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.count("optionalIntValue" + p)));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.count("bigDecimalValue" + p)));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.count("bigIntegerValue" + p)));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.count("comparableValue" + p)));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.count("optionalComparableValue" + p)));
    }

    public static void assertDistinctAggregators(IMap<Integer, Person> map, String p) {
        // projections do not support [any] but we have one element only so here we go.
        assertNoDataMissing(map, PERSONS_COUNT);
        String projection = p.contains("[any]") ? "[0]" : "";
        assertCollectionEquals(map.project(Projections.singleAttribute("doubleValue" + projection)),
                map.aggregate(Aggregators.distinct("doubleValue" + p)));
        assertCollectionEquals(map.project(Projections.singleAttribute("longValue" + projection)),
                map.aggregate(Aggregators.distinct("longValue" + p)));
        assertCollectionEquals(map.project(Projections.singleAttribute("intValue" + projection)),
                map.aggregate(Aggregators.distinct("intValue" + p)));
        assertCollectionEquals(map.project(Projections.singleAttribute("optionalIntValue" + projection)),
                map.aggregate(Aggregators.distinct("optionalIntValue" + p)));
        assertCollectionEquals(map.project(Projections.singleAttribute("bigDecimalValue" + projection)),
                map.aggregate(Aggregators.distinct("bigDecimalValue" + p)));
        assertCollectionEquals(map.project(Projections.singleAttribute("bigIntegerValue" + projection)),
                map.aggregate(Aggregators.distinct("bigIntegerValue" + p)));
        assertCollectionEquals(map.project(Projections.singleAttribute("comparableValue" + projection)),
                map.aggregate(Aggregators.distinct("comparableValue" + p)));
        assertCollectionEquals(map.project(Projections.singleAttribute("optionalComparableValue" + projection)),
                map.aggregate(Aggregators.distinct("optionalComparableValue" + p)));
    }

    @Test
    public void testAggregators_nullCornerCases() {
        IMap<Integer, Person> map = getMapWithNodeCount(3, parallelAccumulation);
        map.put(0, postfix.contains("[any]") ? PersonAny.nulls() : new Person());

        if (postfix.contains("[any]")) {
            assertMinAggregatorsAnyCornerCase(map, postfix);
            assertMaxAggregatorsAnyCornerCase(map, postfix);
            assertSumAggregatorsAnyCornerCase(map, postfix);
            assertAverageAggregatorsAnyCornerCase(map, postfix);
            assertCountAggregatorsAnyCornerCase(map, postfix, 0);
            assertDistinctAggregatorsAnyCornerCase(map, postfix, emptySet());
        } else {
            assertMinAggregatorsAnyCornerCase(map, postfix);
            assertMaxAggregatorsAnyCornerCase(map, postfix);
            // sum and avg do not accept null values, thus skipped
            assertCountAggregatorsAnyCornerCase(map, postfix, 1);
            HashSet<?> expected = new HashSet<>();
            expected.add(null);
            assertDistinctAggregatorsAnyCornerCase(map, postfix, expected);
        }
    }

    @Test
    public void testAggregators_emptyCornerCases() {
        IMap<Integer, Person> map = getMapWithNodeCount(3, parallelAccumulation);

        if (postfix.contains("[any]")) {
            map.put(0, PersonAny.empty());
            assertMinAggregatorsAnyCornerCase(map, postfix);
            assertMaxAggregatorsAnyCornerCase(map, postfix);
            assertSumAggregatorsAnyCornerCase(map, postfix);
            assertAverageAggregatorsAnyCornerCase(map, postfix);
            assertCountAggregatorsAnyCornerCase(map, postfix, 0);
            assertDistinctAggregatorsAnyCornerCase(map, postfix, emptySet());
        }
    }

    private void assertMinAggregatorsAnyCornerCase(IMap<Integer, Person> map, String p) {
        assertNoDataMissing(map, 1);
        assertNull(map.aggregate(Aggregators.doubleMin("doubleValue" + p)));
        assertNull(map.aggregate(Aggregators.longMin("longValue" + p)));
        assertNull(map.aggregate(Aggregators.integerMin("intValue" + p)));
        assertNull(map.aggregate(Aggregators.integerMin("optionalIntValue" + p)));
        assertNull(map.aggregate(Aggregators.bigDecimalMin("bigDecimalValue" + p)));
        assertNull(map.aggregate(Aggregators.bigIntegerMin("bigIntegerValue" + p)));

        assertNull(map.aggregate(Aggregators.comparableMin("doubleValue" + p)));
        assertNull(map.aggregate(Aggregators.comparableMin("longValue" + p)));
        assertNull(map.aggregate(Aggregators.comparableMin("intValue" + p)));
        assertNull(map.aggregate(Aggregators.comparableMin("optionalIntValue" + p)));
        assertNull(map.aggregate(Aggregators.comparableMin("bigDecimalValue" + p)));
        assertNull(map.aggregate(Aggregators.comparableMin("bigIntegerValue" + p)));

        assertNull(map.aggregate(Aggregators.comparableMin("comparableValue" + p)));
        assertNull(map.aggregate(Aggregators.comparableMin("optionalComparableValue" + p)));
    }

    public static void assertMaxAggregatorsAnyCornerCase(IMap<Integer, Person> map, String p) {
        assertNoDataMissing(map, 1);
        assertNull(map.aggregate(Aggregators.doubleMax("doubleValue" + p)));
        assertNull(map.aggregate(Aggregators.longMax("longValue" + p)));
        assertNull(map.aggregate(Aggregators.integerMax("intValue" + p)));
        assertNull(map.aggregate(Aggregators.integerMax("optionalIntValue" + p)));
        assertNull(map.aggregate(Aggregators.bigDecimalMax("bigDecimalValue" + p)));
        assertNull(map.aggregate(Aggregators.bigIntegerMax("bigIntegerValue" + p)));

        assertNull(map.aggregate(Aggregators.comparableMax("doubleValue" + p)));
        assertNull(map.aggregate(Aggregators.comparableMax("longValue" + p)));
        assertNull(map.aggregate(Aggregators.comparableMax("intValue" + p)));
        assertNull(map.aggregate(Aggregators.comparableMax("optionalIntValue" + p)));
        assertNull(map.aggregate(Aggregators.comparableMax("bigDecimalValue" + p)));
        assertNull(map.aggregate(Aggregators.comparableMax("bigIntegerValue" + p)));

        assertNull(map.aggregate(Aggregators.comparableMax("comparableValue" + p)));
        assertNull(map.aggregate(Aggregators.comparableMax("optionalComparableValue" + p)));
    }

    public static void assertSumAggregatorsAnyCornerCase(IMap<Integer, Person> map, String p) {
        assertNoDataMissing(map, 1);
        assertEquals(Double.valueOf(0), map.aggregate(Aggregators.doubleSum("doubleValue" + p)));
        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.longSum("longValue" + p)));
        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.integerSum("intValue" + p)));
        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.integerSum("optionalIntValue" + p)));
        assertEquals(BigDecimal.valueOf(0), map.aggregate(Aggregators.bigDecimalSum("bigDecimalValue" + p)));
        assertEquals(BigInteger.valueOf(0), map.aggregate(Aggregators.bigIntegerSum("bigIntegerValue" + p)));

        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.fixedPointSum("doubleValue" + p)));
        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.fixedPointSum("longValue" + p)));
        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.fixedPointSum("intValue" + p)));
        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.fixedPointSum("optionalIntValue" + p)));
        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.fixedPointSum("bigIntegerValue" + p)));
        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.fixedPointSum("bigDecimalValue" + p)));

        assertEquals(Double.valueOf(0), map.aggregate(Aggregators.floatingPointSum("doubleValue" + p)));
        assertEquals(Double.valueOf(0), map.aggregate(Aggregators.floatingPointSum("longValue" + p)));
        assertEquals(Double.valueOf(0), map.aggregate(Aggregators.floatingPointSum("intValue" + p)));
        assertEquals(Double.valueOf(0), map.aggregate(Aggregators.floatingPointSum("optionalIntValue" + p)));
        assertEquals(Double.valueOf(0), map.aggregate(Aggregators.floatingPointSum("bigIntegerValue" + p)));
        assertEquals(Double.valueOf(0), map.aggregate(Aggregators.floatingPointSum("bigDecimalValue" + p)));
    }

    public static void assertAverageAggregatorsAnyCornerCase(IMap<Integer, Person> map, String p) {
        assertNoDataMissing(map, 1);
        assertNull(map.aggregate(Aggregators.doubleAvg("doubleValue" + p)));
        assertNull(map.aggregate(Aggregators.longAvg("longValue" + p)));
        assertNull(map.aggregate(Aggregators.integerAvg("intValue" + p)));
        assertNull(map.aggregate(Aggregators.integerAvg("optionalIntValue" + p)));
        assertNull(map.aggregate(Aggregators.bigDecimalAvg("bigDecimalValue" + p)));
        assertNull(map.aggregate(Aggregators.bigIntegerAvg("bigIntegerValue" + p)));

        assertNull(map.aggregate(Aggregators.numberAvg("doubleValue" + p)));
        assertNull(map.aggregate(Aggregators.numberAvg("longValue" + p)));
        assertNull(map.aggregate(Aggregators.numberAvg("intValue" + p)));
        assertNull(map.aggregate(Aggregators.numberAvg("optionalIntValue" + p)));
        assertNull(map.aggregate(Aggregators.numberAvg("bigDecimalValue" + p)));
        assertNull(map.aggregate(Aggregators.numberAvg("bigIntegerValue" + p)));
    }

    public static void assertCountAggregatorsAnyCornerCase(IMap<Integer, Person> map, String p, long value) {
        assertNoDataMissing(map, 1);
        assertEquals(Long.valueOf(value), map.aggregate(Aggregators.count("doubleValue" + p)));
        assertEquals(Long.valueOf(value), map.aggregate(Aggregators.count("longValue" + p)));
        assertEquals(Long.valueOf(value), map.aggregate(Aggregators.count("intValue" + p)));
        assertEquals(Long.valueOf(value), map.aggregate(Aggregators.count("optionalIntValue" + p)));
        assertEquals(Long.valueOf(value), map.aggregate(Aggregators.count("bigDecimalValue" + p)));
        assertEquals(Long.valueOf(value), map.aggregate(Aggregators.count("bigIntegerValue" + p)));
        assertEquals(Long.valueOf(value), map.aggregate(Aggregators.count("comparableValue" + p)));
        assertEquals(Long.valueOf(value), map.aggregate(Aggregators.count("optionalComparableValue" + p)));
    }

    public static void assertDistinctAggregatorsAnyCornerCase(IMap<Integer, Person> map, String p, Set result) {
        assertNoDataMissing(map, 1);
        assertEquals(result, map.aggregate(Aggregators.distinct("doubleValue" + p)));
        assertEquals(result, map.aggregate(Aggregators.distinct("longValue" + p)));
        assertEquals(result, map.aggregate(Aggregators.distinct("intValue" + p)));
        assertEquals(result, map.aggregate(Aggregators.distinct("optionalIntValue" + p)));
        assertEquals(result, map.aggregate(Aggregators.distinct("bigDecimalValue" + p)));
        assertEquals(result, map.aggregate(Aggregators.distinct("bigIntegerValue" + p)));
        assertEquals(result, map.aggregate(Aggregators.distinct("comparableValue" + p)));
        assertEquals(result, map.aggregate(Aggregators.distinct("optionalComparableValue" + p)));
    }

    protected <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount, boolean parallelAccumulation) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count < 1");
        }

        MapConfig mapConfig = new MapConfig()
                .setName("aggr")
                .setInMemoryFormat(inMemoryFormat);

        Config config = getConfig()
                .setProperty(PARTITION_COUNT.getName(), String.valueOf(nodeCount))
                .setProperty(AGGREGATION_ACCUMULATION_PARALLEL_EVALUATION.getName(), String.valueOf(parallelAccumulation))
                .addMapConfig(mapConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        HazelcastInstance instance = factory.newInstances(config)[0];
        return instance.getMap("aggr");
    }

    private static void assertCollectionEquals(Collection<?> a, Collection<?> b) {
        TreeSet<?> aSorted = new TreeSet<>(a);
        TreeSet<?> bSorted = new TreeSet<>(b);
        assertEquals(aSorted, bSorted);
    }

    private static void assertNoDataMissing(IMap<Integer, Person> map, int expectedSize) {
        assertEquals("There is missing data in the map!", expectedSize, map.size());
    }

    public static void populateMapWithPersons(IMap<Integer, Person> map, String postfix, int count) {
        for (int i = 1; i <= count; i++) {
            map.put(i, postfix.contains("[any]") ? new PersonAny(i) : new Person(i));
        }
        assertNoDataMissing(map, count);
    }

    @SuppressWarnings("WeakerAccess")
    public static class Person implements Serializable {

        public Integer intValue;
        public Double doubleValue;
        public Long longValue;
        public BigDecimal bigDecimalValue;
        public BigInteger bigIntegerValue;
        public String comparableValue;

        public Person() {
        }

        public Person(int numberValue) {
            this.intValue = numberValue;
            this.doubleValue = (double) numberValue;
            this.longValue = (long) numberValue;
            this.bigDecimalValue = BigDecimal.valueOf(numberValue);
            this.bigIntegerValue = BigInteger.valueOf(numberValue);
            this.comparableValue = String.valueOf(numberValue);
        }

        @SuppressWarnings("unused")
        public Optional getOptionalIntValue() {
            return Optional.ofNullable(intValue);
        }

        @SuppressWarnings("unused")
        public Optional getOptionalComparableValue() {
            return Optional.ofNullable(comparableValue);
        }

    }

    @SuppressWarnings("WeakerAccess")
    public static class PersonAny extends Person implements Serializable {

        public int[] intValue;
        public double[] doubleValue;
        public long[] longValue;
        public List<BigDecimal> bigDecimalValue;
        public List<BigInteger> bigIntegerValue;
        public List<String> comparableValue;

        public PersonAny() {
        }

        public PersonAny(int numberValue) {
            this.intValue = new int[]{numberValue};
            this.doubleValue = new double[]{numberValue};
            this.longValue = new long[]{numberValue};
            this.bigDecimalValue = singletonList(BigDecimal.valueOf(numberValue));
            this.bigIntegerValue = singletonList(BigInteger.valueOf(numberValue));
            this.comparableValue = singletonList(String.valueOf(numberValue));
        }

        @Override
        public Optional getOptionalIntValue() {
            return Optional.ofNullable(intValue);
        }

        @Override
        public Optional getOptionalComparableValue() {
            return Optional.ofNullable(comparableValue);
        }

        public static PersonAny empty() {
            PersonAny person = new PersonAny();
            person.intValue = new int[]{};
            person.doubleValue = new double[]{};
            person.longValue = new long[]{};
            person.bigDecimalValue = new ArrayList<>();
            person.bigIntegerValue = new ArrayList<>();
            person.comparableValue = new ArrayList<>();
            return person;
        }

        public static PersonAny nulls() {
            return new PersonAny();
        }

    }

}
