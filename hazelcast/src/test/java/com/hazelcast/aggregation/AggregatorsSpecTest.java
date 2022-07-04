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

package com.hazelcast.aggregation;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static com.hazelcast.spi.properties.ClusterProperty.AGGREGATION_ACCUMULATION_PARALLEL_EVALUATION;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParametrizedRunner.class)
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

    @Parameter(3)
    public boolean useIndex;

    @Parameter(4)
    public boolean usePredicate;

    private Predicate<Integer, Person> predicate;

    @Parameters(name = "{0} parallelAccumulation={1}, postfix={2}, useIndex={3}, usePredicate={4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, false, "", true, true},
                {InMemoryFormat.OBJECT, false, "", true, true},
                {InMemoryFormat.BINARY, true, "", true, true},
                {InMemoryFormat.OBJECT, true, "", true, true},
                {InMemoryFormat.BINARY, false, "[any]", true, true},
                {InMemoryFormat.OBJECT, false, "[any]", true, true},
                {InMemoryFormat.BINARY, true, "[any]", true, true},
                {InMemoryFormat.OBJECT, true, "[any]", true, true},
                {InMemoryFormat.BINARY, false, "", false, true},
                {InMemoryFormat.OBJECT, false, "", false, true},
                {InMemoryFormat.BINARY, true, "", false, true},
                {InMemoryFormat.OBJECT, true, "", false, true},
                {InMemoryFormat.BINARY, false, "[any]", false, true},
                {InMemoryFormat.OBJECT, false, "[any]", false, true},
                {InMemoryFormat.BINARY, true, "[any]", false, true},
                {InMemoryFormat.OBJECT, true, "[any]", false, true},

                // we skip combinations where (format=*, parallel=*, postfix=*, useIndex=true, usePredicate=false)
                // because it's pointless to create an index when a predicate is not used
                {InMemoryFormat.BINARY, false, "", false, false},
                {InMemoryFormat.OBJECT, false, "", false, false},
                {InMemoryFormat.BINARY, true, "", false, false},
                {InMemoryFormat.OBJECT, true, "", false, false},
                {InMemoryFormat.BINARY, false, "[any]", false, false},
                {InMemoryFormat.OBJECT, false, "[any]", false, false},
                {InMemoryFormat.BINARY, true, "[any]", false, false},
                {InMemoryFormat.OBJECT, true, "[any]", false, false},
        });
    }

    @Before
    public void setUp() {
        predicate = usePredicate
                ? Predicates.greaterEqual("fieldWeCanQuery", Integer.MAX_VALUE) : Predicates.alwaysTrue();
    }

    @Test
    public void testAggregators() {
        IMap<Integer, Person> map = getMapWithNodeCount(3, parallelAccumulation, useIndex);
        populateMapWithPersons(map, postfix, PERSONS_COUNT);

        assertMinAggregators(map, postfix, predicate);
        assertMaxAggregators(map, postfix, predicate);
        assertSumAggregators(map, postfix, predicate);
        assertAverageAggregators(map, postfix, predicate);
        assertCountAggregators(map, postfix, predicate);
        assertDistinctAggregators(map, postfix, predicate);
        assertMaxByAggregators(map, postfix, predicate);
        assertMinByAggregators(map, postfix, predicate);
    }

    private static void assertMaxByAggregators(IMap<Integer, Person> map, String p, Predicate<Integer, Person> predicate) {
        assertKeyEquals(999, map.aggregate(Aggregators.maxBy("intValue" + p), predicate));
        assertKeyEquals(999, map.aggregate(Aggregators.maxBy("doubleValue" + p), predicate));
        assertKeyEquals(999, map.aggregate(Aggregators.maxBy("longValue" + p), predicate));
        assertKeyEquals(999, map.aggregate(Aggregators.maxBy("bigDecimalValue" + p), predicate));
        assertKeyEquals(999, map.aggregate(Aggregators.maxBy("bigIntegerValue" + p), predicate));
        assertKeyEquals(999, map.aggregate(Aggregators.maxBy("comparableValue" + p), predicate));
        assertKeyEquals(999, map.aggregate(Aggregators.maxBy("optionalComparableValue" + p), predicate));
    }

    private static void assertMinByAggregators(IMap<Integer, Person> map, String p, Predicate<Integer, Person> predicate) {
        assertKeyEquals(1, map.aggregate(Aggregators.minBy("intValue" + p), predicate));
        assertKeyEquals(1, map.aggregate(Aggregators.minBy("doubleValue" + p), predicate));
        assertKeyEquals(1, map.aggregate(Aggregators.minBy("longValue" + p), predicate));
        assertKeyEquals(1, map.aggregate(Aggregators.minBy("bigDecimalValue" + p), predicate));
        assertKeyEquals(1, map.aggregate(Aggregators.minBy("bigIntegerValue" + p), predicate));
        assertKeyEquals(1, map.aggregate(Aggregators.minBy("comparableValue" + p), predicate));
        assertKeyEquals(1, map.aggregate(Aggregators.minBy("optionalComparableValue" + p), predicate));
    }

    public static void assertMinAggregators(IMap<Integer, Person> map, String p, Predicate<Integer, Person> predicate) {
        assertNoDataMissing(map, PERSONS_COUNT);
        assertEquals(Double.valueOf(1), map.aggregate(Aggregators.doubleMin("doubleValue" + p), predicate));
        assertEquals(Long.valueOf(1), map.aggregate(Aggregators.longMin("longValue" + p), predicate));
        assertEquals(Integer.valueOf(1), map.aggregate(Aggregators.integerMin("intValue" + p), predicate));
        assertEquals(Integer.valueOf(1), map.aggregate(Aggregators.integerMin("optionalIntValue" + p), predicate));
        assertEquals(BigDecimal.valueOf(1), map.aggregate(Aggregators.bigDecimalMin("bigDecimalValue" + p), predicate));
        assertEquals(BigInteger.valueOf(1), map.aggregate(Aggregators.bigIntegerMin("bigIntegerValue" + p), predicate));

        assertEquals(Double.valueOf(1), map.aggregate(Aggregators.comparableMin("doubleValue" + p), predicate));
        assertEquals(Long.valueOf(1), map.aggregate(Aggregators.comparableMin("longValue" + p), predicate));
        assertEquals(Integer.valueOf(1), map.aggregate(Aggregators.comparableMin("intValue" + p), predicate));
        assertEquals(Integer.valueOf(1), map.aggregate(Aggregators.comparableMin("optionalIntValue" + p), predicate));
        assertEquals(BigDecimal.valueOf(1), map.aggregate(Aggregators.comparableMin("bigDecimalValue" + p), predicate));
        assertEquals(BigInteger.valueOf(1), map.aggregate(Aggregators.comparableMin("bigIntegerValue" + p), predicate));

        assertEquals("1", map.aggregate(Aggregators.comparableMin("comparableValue" + p), predicate));
        assertEquals("1", map.aggregate(Aggregators.comparableMin("optionalComparableValue" + p), predicate));
    }

    public static void assertMaxAggregators(IMap<Integer, Person> map, String p, Predicate<Integer, Person> predicate) {
        assertNoDataMissing(map, PERSONS_COUNT);
        assertEquals(Double.valueOf(999), map.aggregate(Aggregators.doubleMax("doubleValue" + p), predicate));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.longMax("longValue" + p), predicate));
        assertEquals(Integer.valueOf(999), map.aggregate(Aggregators.integerMax("intValue" + p), predicate));
        assertEquals(Integer.valueOf(999), map.aggregate(Aggregators.integerMax("optionalIntValue" + p), predicate));
        assertEquals(BigDecimal.valueOf(999), map.aggregate(Aggregators.bigDecimalMax("bigDecimalValue" + p), predicate));
        assertEquals(BigInteger.valueOf(999), map.aggregate(Aggregators.bigIntegerMax("bigIntegerValue" + p), predicate));

        assertEquals(Double.valueOf(999), map.aggregate(Aggregators.comparableMax("doubleValue" + p), predicate));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.comparableMax("longValue" + p), predicate));
        assertEquals(Integer.valueOf(999), map.aggregate(Aggregators.comparableMax("intValue" + p), predicate));
        assertEquals(Integer.valueOf(999), map.aggregate(Aggregators.comparableMax("optionalIntValue" + p), predicate));
        assertEquals(BigDecimal.valueOf(999), map.aggregate(Aggregators.comparableMax("bigDecimalValue" + p), predicate));
        assertEquals(BigInteger.valueOf(999), map.aggregate(Aggregators.comparableMax("bigIntegerValue" + p), predicate));

        assertEquals("999", map.aggregate(Aggregators.comparableMax("comparableValue" + p), predicate));
        assertEquals("999", map.aggregate(Aggregators.comparableMax("optionalComparableValue" + p), predicate));
    }

    public static void assertSumAggregators(IMap<Integer, Person> map, String p, Predicate<Integer, Person> predicate) {
        assertNoDataMissing(map, PERSONS_COUNT);
        assertEquals(Double.valueOf(499500.0d), map.aggregate(Aggregators.doubleSum("doubleValue" + p), predicate));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.longSum("longValue" + p), predicate));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.integerSum("intValue" + p), predicate));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.integerSum("optionalIntValue" + p), predicate));
        assertEquals(BigDecimal.valueOf(499500), map.aggregate(Aggregators.bigDecimalSum("bigDecimalValue" + p), predicate));
        assertEquals(BigInteger.valueOf(499500), map.aggregate(Aggregators.bigIntegerSum("bigIntegerValue" + p), predicate));

        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.fixedPointSum("doubleValue" + p), predicate));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.fixedPointSum("longValue" + p), predicate));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.fixedPointSum("intValue" + p), predicate));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.fixedPointSum("optionalIntValue" + p), predicate));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.fixedPointSum("bigIntegerValue" + p), predicate));
        assertEquals(Long.valueOf(499500), map.aggregate(Aggregators.fixedPointSum("bigDecimalValue" + p), predicate));

        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.floatingPointSum("doubleValue" + p), predicate));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.floatingPointSum("longValue" + p), predicate));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.floatingPointSum("intValue" + p), predicate));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.floatingPointSum("optionalIntValue" + p), predicate));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.floatingPointSum("bigIntegerValue" + p), predicate));
        assertEquals(Double.valueOf(499500), map.aggregate(Aggregators.floatingPointSum("bigDecimalValue" + p), predicate));
    }

    public static void assertAverageAggregators(IMap<Integer, Person> map, String p, Predicate<Integer, Person> predicate) {
        assertNoDataMissing(map, PERSONS_COUNT);
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.doubleAvg("doubleValue" + p), predicate));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.longAvg("longValue" + p), predicate));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.integerAvg("intValue" + p), predicate));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.integerAvg("optionalIntValue" + p), predicate));
        assertEquals(BigDecimal.valueOf(500), map.aggregate(Aggregators.bigDecimalAvg("bigDecimalValue" + p), predicate));
        assertEquals(BigDecimal.valueOf(500), map.aggregate(Aggregators.bigIntegerAvg("bigIntegerValue" + p), predicate));

        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.numberAvg("doubleValue" + p), predicate));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.numberAvg("longValue" + p), predicate));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.numberAvg("intValue" + p), predicate));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.numberAvg("optionalIntValue" + p), predicate));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.numberAvg("bigDecimalValue" + p), predicate));
        assertEquals(Double.valueOf(500.0d), map.aggregate(Aggregators.numberAvg("bigIntegerValue" + p), predicate));
    }

    public static void assertCountAggregators(IMap<Integer, Person> map, String p, Predicate<Integer, Person> predicate) {
        assertNoDataMissing(map, PERSONS_COUNT);
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.count("doubleValue" + p), predicate));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.count("longValue" + p), predicate));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.count("intValue" + p), predicate));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.count("optionalIntValue" + p), predicate));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.count("bigDecimalValue" + p), predicate));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.count("bigIntegerValue" + p), predicate));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.count("comparableValue" + p), predicate));
        assertEquals(Long.valueOf(999), map.aggregate(Aggregators.count("optionalComparableValue" + p), predicate));
    }

    public static void assertDistinctAggregators(IMap<Integer, Person> map, String p, Predicate<Integer, Person> predicate) {
        // projections do not support [any] but we have one element only so here we go.
        assertNoDataMissing(map, PERSONS_COUNT);
        String projection = p.contains("[any]") ? "[0]" : "";
        assertCollectionEquals(map.project(Projections.singleAttribute("doubleValue" + projection)),
                map.aggregate(Aggregators.distinct("doubleValue" + p), predicate));
        assertCollectionEquals(map.project(Projections.singleAttribute("longValue" + projection)),
                map.aggregate(Aggregators.distinct("longValue" + p), predicate));
        assertCollectionEquals(map.project(Projections.singleAttribute("intValue" + projection)),
                map.aggregate(Aggregators.distinct("intValue" + p), predicate));
        assertCollectionEquals(map.project(Projections.singleAttribute("optionalIntValue" + projection)),
                map.aggregate(Aggregators.distinct("optionalIntValue" + p), predicate));
        assertCollectionEquals(map.project(Projections.singleAttribute("bigDecimalValue" + projection)),
                map.aggregate(Aggregators.distinct("bigDecimalValue" + p), predicate));
        assertCollectionEquals(map.project(Projections.singleAttribute("bigIntegerValue" + projection)),
                map.aggregate(Aggregators.distinct("bigIntegerValue" + p), predicate));
        assertCollectionEquals(map.project(Projections.singleAttribute("comparableValue" + projection)),
                map.aggregate(Aggregators.distinct("comparableValue" + p), predicate));
        assertCollectionEquals(map.project(Projections.singleAttribute("optionalComparableValue" + projection)),
                map.aggregate(Aggregators.distinct("optionalComparableValue" + p), predicate));
    }

    @Test
    public void testAggregators_nullCornerCases() {
        IMap<Integer, Person> map = getMapWithNodeCount(3, parallelAccumulation, useIndex);
        map.put(0, postfix.contains("[any]") ? PersonAny.nulls() : new Person());

        if (postfix.contains("[any]")) {
            assertMinAggregatorsAnyCornerCase(map, postfix, predicate);
            assertMaxAggregatorsAnyCornerCase(map, postfix, predicate);
            assertSumAggregatorsAnyCornerCase(map, postfix, predicate);
            assertAverageAggregatorsAnyCornerCase(map, postfix, predicate);
            assertCountAggregatorsAnyCornerCase(map, postfix, 0, predicate);
            assertDistinctAggregatorsAnyCornerCase(map, postfix, emptySet(), predicate);
            assertMinByAggregatorAnyCornerCase(map, postfix, predicate);
            assertMaxByAggregatorAnyCornerCase(map, postfix, predicate);
        } else {
            assertMinAggregatorsAnyCornerCase(map, postfix, predicate);
            assertMaxAggregatorsAnyCornerCase(map, postfix, predicate);
            // sum and avg do not accept null values, thus skipped
            assertCountAggregatorsAnyCornerCase(map, postfix, 1, predicate);
            HashSet<?> expected = new HashSet<>();
            expected.add(null);
            assertDistinctAggregatorsAnyCornerCase(map, postfix, expected, predicate);
            assertMinByAggregatorAnyCornerCase(map, postfix, predicate);
            assertMaxByAggregatorAnyCornerCase(map, postfix, predicate);
        }
    }

    @Test
    public void testAggregators_emptyCornerCases() {
        IMap<Integer, Person> map = getMapWithNodeCount(3, parallelAccumulation, useIndex);

        if (postfix.contains("[any]")) {
            map.put(0, PersonAny.empty());
            assertMinAggregatorsAnyCornerCase(map, postfix, predicate);
            assertMaxAggregatorsAnyCornerCase(map, postfix, predicate);
            assertSumAggregatorsAnyCornerCase(map, postfix, predicate);
            assertAverageAggregatorsAnyCornerCase(map, postfix, predicate);
            assertCountAggregatorsAnyCornerCase(map, postfix, 0, predicate);
            assertDistinctAggregatorsAnyCornerCase(map, postfix, emptySet(), predicate);
            assertMinByAggregatorAnyCornerCase(map, postfix, predicate);
            assertMaxByAggregatorAnyCornerCase(map, postfix, predicate);
        }
    }

    private static void assertMinByAggregatorAnyCornerCase(IMap<Integer, Person> map, String p,  Predicate<Integer, Person> predicate) {
        assertNoDataMissing(map, 1);
        assertNull(map.aggregate(Aggregators.minBy("doubleValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.minBy("longValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.minBy("intValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.minBy("optionalIntValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.minBy("bigDecimalValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.minBy("bigIntegerValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.minBy("comparableValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.minBy("optionalComparableValue" + p), predicate));
    }

    private static void assertMaxByAggregatorAnyCornerCase(IMap<Integer, Person> map, String p,  Predicate<Integer, Person> predicate) {
        assertNoDataMissing(map, 1);
        assertNull(map.aggregate(Aggregators.maxBy("doubleValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.maxBy("longValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.maxBy("intValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.maxBy("optionalIntValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.maxBy("bigDecimalValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.maxBy("bigIntegerValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.maxBy("comparableValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.maxBy("optionalComparableValue" + p), predicate));
    }

    private static void assertMinAggregatorsAnyCornerCase(IMap<Integer, Person> map, String p,  Predicate<Integer, Person> predicate) {
        assertNoDataMissing(map, 1);
        assertNull(map.aggregate(Aggregators.doubleMin("doubleValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.longMin("longValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.integerMin("intValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.integerMin("optionalIntValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.bigDecimalMin("bigDecimalValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.bigIntegerMin("bigIntegerValue" + p), predicate));

        assertNull(map.aggregate(Aggregators.comparableMin("doubleValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.comparableMin("longValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.comparableMin("intValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.comparableMin("optionalIntValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.comparableMin("bigDecimalValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.comparableMin("bigIntegerValue" + p), predicate));

        assertNull(map.aggregate(Aggregators.comparableMin("comparableValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.comparableMin("optionalComparableValue" + p), predicate));
    }

    public static void assertMaxAggregatorsAnyCornerCase(IMap<Integer, Person> map, String p, Predicate<Integer, Person> predicate) {
        assertNoDataMissing(map, 1);
        assertNull(map.aggregate(Aggregators.doubleMax("doubleValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.longMax("longValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.integerMax("intValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.integerMax("optionalIntValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.bigDecimalMax("bigDecimalValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.bigIntegerMax("bigIntegerValue" + p), predicate));

        assertNull(map.aggregate(Aggregators.comparableMax("doubleValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.comparableMax("longValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.comparableMax("intValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.comparableMax("optionalIntValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.comparableMax("bigDecimalValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.comparableMax("bigIntegerValue" + p), predicate));

        assertNull(map.aggregate(Aggregators.comparableMax("comparableValue" + p), predicate));
        assertNull(map.aggregate(Aggregators.comparableMax("optionalComparableValue" + p), predicate));
    }

    public static void assertSumAggregatorsAnyCornerCase(IMap<Integer, Person> map, String p, Predicate<Integer, Person> predicate) {
        assertNoDataMissing(map, 1);
        assertEquals(Double.valueOf(0), map.aggregate(Aggregators.doubleSum("doubleValue" + p), predicate));
        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.longSum("longValue" + p), predicate));
        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.integerSum("intValue" + p), predicate));
        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.integerSum("optionalIntValue" + p), predicate));
        assertEquals(BigDecimal.valueOf(0), map.aggregate(Aggregators.bigDecimalSum("bigDecimalValue" + p), predicate));
        assertEquals(BigInteger.valueOf(0), map.aggregate(Aggregators.bigIntegerSum("bigIntegerValue" + p), predicate));

        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.fixedPointSum("doubleValue" + p), predicate));
        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.fixedPointSum("longValue" + p), predicate));
        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.fixedPointSum("intValue" + p), predicate));
        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.fixedPointSum("optionalIntValue" + p), predicate));
        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.fixedPointSum("bigIntegerValue" + p), predicate));
        assertEquals(Long.valueOf(0), map.aggregate(Aggregators.fixedPointSum("bigDecimalValue" + p), predicate));

        assertEquals(Double.valueOf(0), map.aggregate(Aggregators.floatingPointSum("doubleValue" + p), predicate));
        assertEquals(Double.valueOf(0), map.aggregate(Aggregators.floatingPointSum("longValue" + p), predicate));
        assertEquals(Double.valueOf(0), map.aggregate(Aggregators.floatingPointSum("intValue" + p), predicate));
        assertEquals(Double.valueOf(0), map.aggregate(Aggregators.floatingPointSum("optionalIntValue" + p), predicate));
        assertEquals(Double.valueOf(0), map.aggregate(Aggregators.floatingPointSum("bigIntegerValue" + p), predicate));
        assertEquals(Double.valueOf(0), map.aggregate(Aggregators.floatingPointSum("bigDecimalValue" + p), predicate));
    }

    public static void assertAverageAggregatorsAnyCornerCase(IMap<Integer, Person> map, String p, Predicate<Integer, Person> predicate) {
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

    public static void assertCountAggregatorsAnyCornerCase(IMap<Integer, Person> map, String p, long value,
                                                           Predicate<Integer, Person> predicate) {
        assertNoDataMissing(map, 1);
        assertEquals(Long.valueOf(value), map.aggregate(Aggregators.count("doubleValue" + p), predicate));
        assertEquals(Long.valueOf(value), map.aggregate(Aggregators.count("longValue" + p), predicate));
        assertEquals(Long.valueOf(value), map.aggregate(Aggregators.count("intValue" + p), predicate));
        assertEquals(Long.valueOf(value), map.aggregate(Aggregators.count("optionalIntValue" + p), predicate));
        assertEquals(Long.valueOf(value), map.aggregate(Aggregators.count("bigDecimalValue" + p), predicate));
        assertEquals(Long.valueOf(value), map.aggregate(Aggregators.count("bigIntegerValue" + p), predicate));
        assertEquals(Long.valueOf(value), map.aggregate(Aggregators.count("comparableValue" + p), predicate));
        assertEquals(Long.valueOf(value), map.aggregate(Aggregators.count("optionalComparableValue" + p), predicate));
    }

    public static void assertDistinctAggregatorsAnyCornerCase(IMap<Integer, Person> map, String p, Set result, Predicate<Integer, Person> predicate) {
        assertNoDataMissing(map, 1);
        assertEquals(result, map.aggregate(Aggregators.distinct("doubleValue" + p), predicate));
        assertEquals(result, map.aggregate(Aggregators.distinct("longValue" + p), predicate));
        assertEquals(result, map.aggregate(Aggregators.distinct("intValue" + p), predicate));
        assertEquals(result, map.aggregate(Aggregators.distinct("optionalIntValue" + p), predicate));
        assertEquals(result, map.aggregate(Aggregators.distinct("bigDecimalValue" + p), predicate));
        assertEquals(result, map.aggregate(Aggregators.distinct("bigIntegerValue" + p), predicate));
        assertEquals(result, map.aggregate(Aggregators.distinct("comparableValue" + p), predicate));
        assertEquals(result, map.aggregate(Aggregators.distinct("optionalComparableValue" + p), predicate));
    }

    protected <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount, boolean parallelAccumulation, boolean useIndex) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count < 1");
        }

        MapConfig mapConfig = new MapConfig()
                .setName("aggr")
                .setInMemoryFormat(inMemoryFormat);

        if (useIndex) {
            mapConfig.addIndexConfig(new IndexConfig(IndexConfig.DEFAULT_TYPE, "fieldWeCanQuery"));
        }

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

    private static <T> void assertKeyEquals(T expectedValue, Map.Entry<T, ?> entry) {
        T actualKeyValue = entry.getKey();
        assertEquals(expectedValue, actualKeyValue);
    }

    public static void populateMapWithPersons(IMap<Integer, Person> map, String postfix, int count) {
        for (int i = 1; i <= count; i++) {
            map.put(i, postfix.contains("[any]") ? new PersonAny(i) : new Person(i));
        }
        assertNoDataMissing(map, count);
    }

    @SuppressWarnings("WeakerAccess")
    public static class Person implements Serializable {
        public int fieldWeCanQuery = Integer.MAX_VALUE;

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
