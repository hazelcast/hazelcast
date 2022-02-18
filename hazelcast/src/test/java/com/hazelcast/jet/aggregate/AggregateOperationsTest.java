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

package com.hazelcast.jet.aggregate;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.accumulator.DoubleAccumulator;
import com.hazelcast.jet.accumulator.LinTrendAccumulator;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.accumulator.LongDoubleAccumulator;
import com.hazelcast.jet.accumulator.LongLongAccumulator;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.accumulator.PickAnyAccumulator;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.hazelcast.function.ComparatorEx.naturalOrder;
import static com.hazelcast.function.Functions.entryKey;
import static com.hazelcast.function.Functions.entryValue;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.allOfBuilder;
import static com.hazelcast.jet.aggregate.AggregateOperations.averagingDouble;
import static com.hazelcast.jet.aggregate.AggregateOperations.averagingLong;
import static com.hazelcast.jet.aggregate.AggregateOperations.bottomN;
import static com.hazelcast.jet.aggregate.AggregateOperations.concatenating;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.filtering;
import static com.hazelcast.jet.aggregate.AggregateOperations.flatMapping;
import static com.hazelcast.jet.aggregate.AggregateOperations.groupingBy;
import static com.hazelcast.jet.aggregate.AggregateOperations.linearTrend;
import static com.hazelcast.jet.aggregate.AggregateOperations.mapping;
import static com.hazelcast.jet.aggregate.AggregateOperations.maxBy;
import static com.hazelcast.jet.aggregate.AggregateOperations.minBy;
import static com.hazelcast.jet.aggregate.AggregateOperations.pickAny;
import static com.hazelcast.jet.aggregate.AggregateOperations.reducing;
import static com.hazelcast.jet.aggregate.AggregateOperations.sorting;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingDouble;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.aggregate.AggregateOperations.toList;
import static com.hazelcast.jet.aggregate.AggregateOperations.toMap;
import static com.hazelcast.jet.aggregate.AggregateOperations.toSet;
import static com.hazelcast.jet.aggregate.AggregateOperations.topN;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AggregateOperationsTest {

    private static final InternalSerializationService SERIALIZATION_SERVICE =
            new DefaultSerializationServiceBuilder().build();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void when_counting() {
        validateOp(counting(), LongAccumulator::get,
                null, null, 1L, 2L, 2L);
    }

    @Test
    public void when_summingLong() {
        validateOp(summingLong(Long::longValue), LongAccumulator::get,
                1L, 2L, 1L, 3L, 3L);
    }

    @Test
    public void when_summingDouble() {
        validateOp(summingDouble(Double::doubleValue), DoubleAccumulator::export,
                0.5, 1.5, 0.5, 2.0, 2.0);
    }

    @Test
    public void when_averagingLong() {
        validateOp(averagingLong(Long::longValue), identity(),
                1L, 2L, new LongLongAccumulator(1, 1), new LongLongAccumulator(2, 3), 1.5);
    }

    @Test
    public void when_averagingLong_noInput_then_NaN() {
        // Given
        AggregateOperation1<Long, LongLongAccumulator, Double> aggrOp = averagingLong(Long::longValue);
        LongLongAccumulator acc = aggrOp.createFn().get();

        // When
        double result = aggrOp.finishFn().apply(acc);

        // Then
        assertEquals(Double.NaN, result, 0.0);
    }

    @Test
    public void when_averagingLong_tooManyItems_then_exception() {
        // Given
        AggregateOperation1<Long, LongLongAccumulator, Double> aggrOp = averagingLong(Long::longValue);
        LongLongAccumulator acc = new LongLongAccumulator(Long.MAX_VALUE, 0L);

        // Then
        exception.expect(ArithmeticException.class);

        // When
        aggrOp.accumulateFn().accept(acc, 0L);
    }

    @Test
    public void when_averagingLong_sumTooLarge_then_exception() {
        // Given
        AggregateOperation1<Long, LongLongAccumulator, Double> aggrOp = averagingLong(Long::longValue);
        LongLongAccumulator acc = aggrOp.createFn().get();

        // Then
        exception.expect(ArithmeticException.class);

        // When
        aggrOp.accumulateFn().accept(acc, Long.MAX_VALUE);
        aggrOp.accumulateFn().accept(acc, 1L);
    }

    @Test
    public void when_averagingDouble() {
        validateOp(averagingDouble(Double::doubleValue), identity(),
                1.5, 2.5, new LongDoubleAccumulator(1, 1.5), new LongDoubleAccumulator(2, 4.0), 2.0);
    }

    @Test
    public void when_averagingDouble_tooManyItems_then_exception() {
        // Given
        AggregateOperation1<Double, LongDoubleAccumulator, Double> aggrOp = averagingDouble(Double::doubleValue);
        LongDoubleAccumulator acc = new LongDoubleAccumulator(Long.MAX_VALUE, 0.0d);

        // Then
        exception.expect(ArithmeticException.class);

        // When
        aggrOp.accumulateFn().accept(acc, 0.0d);
    }

    @Test
    public void when_averagingDouble_noInput_then_NaN() {
        // Given
        AggregateOperation1<Double, LongDoubleAccumulator, Double> aggrOp = averagingDouble(Double::doubleValue);
        LongDoubleAccumulator acc = aggrOp.createFn().get();

        // When
        double result = aggrOp.finishFn().apply(acc);

        // Then
        assertEquals(Double.NaN, result, 0.0);
    }

    @Test
    public void when_maxBy() {
        validateOpWithoutDeduct(maxBy(naturalOrder()), MutableReference::get,
                10L, 11L, 10L, 11L, 11L);
    }

    @Test
    public void when_maxBy_noInput_then_nullResult() {
        // Given
        AggregateOperation1<String, MutableReference<String>, String> aggrOp = maxBy(naturalOrder());
        MutableReference<String> acc = aggrOp.createFn().get();

        // When
        String result = aggrOp.finishFn().apply(acc);

        // Then
        assertNull(result);
    }

    @Test
    public void when_topN() {
        AggregateOperationsTest.validateOpWithoutDeduct(topN(2, naturalOrder()),
                ArrayList::new,
                Arrays.asList(7, 1, 5, 3),
                Arrays.asList(8, 6, 2, 4),
                Arrays.asList(5, 7),
                Arrays.asList(7, 8),
                Arrays.asList(8, 7)
        );
    }

    @Test
    public void when_bottomN() {
        AggregateOperationsTest.validateOpWithoutDeduct(bottomN(2, naturalOrder()),
                ArrayList::new,
                Arrays.asList(7, 1, 5, 3),
                Arrays.asList(8, 6, 2, 4),
                Arrays.asList(3, 1),
                Arrays.asList(2, 1),
                Arrays.asList(1, 2)
        );
    }

    @Test
    public void when_minBy() {
        validateOpWithoutDeduct(minBy(naturalOrder()), MutableReference::get,
                10L, 11L, 10L, 10L, 10L);
    }

    @Test
    public void when_allOf2() {
        validateOp(
                allOf(counting(), summingLong(Long::longValue)),
                identity(), 10L, 11L,
                tuple2(longAcc(1), longAcc(10)),
                tuple2(longAcc(2), longAcc(21)),
                tuple2(2L, 21L)
        );
    }

    @Test
    public void when_allOfWithoutDeduct_then_noDeduct() {
        validateOpWithoutDeduct(
                // JDK 11 can't infer <Long>
                allOf(counting(), AggregateOperations.<Long>maxBy(naturalOrder())),
                identity(), 10L, 11L,
                tuple2(longAcc(1), new MutableReference<>(10L)),
                tuple2(longAcc(2), new MutableReference<>(11L)),
                tuple2(2L, 11L)
        );
    }

    @Test
    public void when_allOf3() {
        validateOp(
                allOf(counting(), summingLong(Long::longValue), averagingLong(Long::longValue)),
                identity(), 10L, 11L,
                tuple3(longAcc(1), longAcc(10), longLongAcc(1, 10)),
                tuple3(longAcc(2), longAcc(21), longLongAcc(2, 21)),
                tuple3(2L, 21L, 10.5)
        );
    }

    @Test
    public void when_allOfN() {
        AllOfAggregationBuilder<Long> builder = allOfBuilder();
        Tag<Long> tagSum = builder.add(summingLong(Long::longValue));
        Tag<Long> tagCount = builder.add(counting());
        AggregateOperationsTest.validateOp(
                builder.build(),
                identity(),
                10L, 11L,
                new Object[]{longAcc(10L), longAcc(1L)},
                new Object[]{longAcc(21L), longAcc(2L)},
                ItemsByTag.itemsByTag(tagCount, 2L, tagSum, 21L)
        );
    }

    @Test
    public void when_allOfWithoutCombine_then_noCombine() {
        AggregateOperation1<Long, ?, Tuple2<Long, Long>> composite =
                allOf(AggregateOperation
                                .withCreate(LongAccumulator::new)
                                .<Long>andAccumulate(LongAccumulator::addAllowingOverflow)
                                .andExportFinish(LongAccumulator::get),
                        summingLong(x -> x));
        assertNull(composite.combineFn());
    }

    @Test
    public void when_linearTrend() {
        // Given
        AggregateOperation1<Entry<Long, Long>, LinTrendAccumulator, Double> op =
                linearTrend(Entry::getKey, Entry::getValue);
        Supplier<LinTrendAccumulator> createFn = op.createFn();
        BiConsumer<? super LinTrendAccumulator, ? super Entry<Long, Long>> accFn = op.accumulateFn();
        BiConsumer<? super LinTrendAccumulator, ? super LinTrendAccumulator> combineFn = op.combineFn();
        BiConsumer<? super LinTrendAccumulator, ? super LinTrendAccumulator> deductFn = op.deductFn();
        Function<? super LinTrendAccumulator, ? extends Double> finishFn = op.finishFn();
        assertNotNull(createFn);
        assertNotNull(accFn);
        assertNotNull(combineFn);
        assertNotNull(deductFn);
        assertNotNull(finishFn);

        // When
        LinTrendAccumulator a1 = createFn.get();
        accFn.accept(a1, entry(1L, 3L));
        accFn.accept(a1, entry(2L, 5L));
        assertEquals(2.0, finishFn.apply(a1), Double.MIN_VALUE);

        LinTrendAccumulator a2 = createFn.get();
        accFn.accept(a2, entry(5L, 11L));
        accFn.accept(a2, entry(6L, 13L));
        assertEquals(2.0, finishFn.apply(a2), Double.MIN_VALUE);

        combineFn.accept(a1, a2);
        assertEquals(2.0, finishFn.apply(a1), Double.MIN_VALUE);

        deductFn.accept(a1, a2);
        assertEquals(2.0, finishFn.apply(a1), Double.MIN_VALUE);

        Double result = finishFn.apply(a1);
        assertEquals(Double.valueOf(2), result);

        // When
        LinTrendAccumulator acc = createFn.get();
        // Then
        assertTrue("NaN expected if nothing accumulated", Double.isNaN(finishFn.apply(acc)));

        // When
        accFn.accept(acc, entry(2L, 1L));
        // Then
        assertTrue("NaN expected if just single point accumulated", Double.isNaN(finishFn.apply(acc)));

        // When
        accFn.accept(acc, entry(2L, 1L));
        // Then
        assertTrue("NaN expected if all data points are equal", Double.isNaN(finishFn.apply(acc)));

        // When
        accFn.accept(acc, entry(2L, 2L));
        // Then
        assertTrue("NaN expected if all data points have same x value", Double.isNaN(finishFn.apply(acc)));
    }

    @Test
    public void when_reducing() {
        validateOp(reducing(0, Integer::intValue, Integer::sum, (x, y) -> x - y),
                MutableReference::get,
                1, 2, 1, 3, 3);
    }

    @Test
    public void when_toList() {
        validateOpWithoutDeduct(
                toList(), identity(), 1, 2, singletonList(1), asList(1, 2), asList(1, 2));
    }

    @Test
    public void when_toSet() {
        validateOpWithoutDeduct(
                toSet(), identity(), 1, 2, singleton(1), new HashSet<>(asList(1, 2)), new HashSet<>(asList(1, 2)));
    }

    @Test
    public void when_toMap() {
        Map<Integer, Integer> acced = new HashMap<>();
        acced.put(1, 1);

        Map<Integer, Integer> combined = new HashMap<>(acced);
        combined.put(2, 2);

        validateOpWithoutDeduct(
                toMap(Entry::getKey, Entry::getValue),
                identity(), entry(1, 1), entry(2, 2),
                acced, combined, combined);
    }

    @Test
    public void when_toMapDuplicateAccumulate_then_exception() {
        AggregateOperation1<Entry<Integer, Integer>, Map<Integer, Integer>, Map<Integer, Integer>> op =
                toMap(Entry::getKey, Entry::getValue);

        Map<Integer, Integer> acc = op.createFn().get();
        op.accumulateFn().accept(acc, entry(1, 1));

        exception.expect(IllegalStateException.class);
        op.accumulateFn().accept(acc, entry(1, 2));
    }

    @Test
    public void when_toMapCombinesDuplicates_then_exception() {
        // Given
        AggregateOperation1<Entry<Integer, Integer>, Map<Integer, Integer>, Map<Integer, Integer>> op =
                toMap(Entry::getKey, Entry::getValue);
        BiConsumerEx<? super Map<Integer, Integer>, ? super Map<Integer, Integer>> combineFn = op.combineFn();
        assertNotNull("combineFn", combineFn);
        Map<Integer, Integer> acc1 = op.createFn().get();
        op.accumulateFn().accept(acc1, entry(1, 1));
        Map<Integer, Integer> acc2 = op.createFn().get();
        op.accumulateFn().accept(acc2, entry(1, 2));

        // Then
        exception.expect(IllegalStateException.class);

        // When
        combineFn.accept(acc1, acc2);
    }

    @Test
    public void when_toMapWithMerge_then_merged() {
        Map<Integer, Integer> acced = new HashMap<>();
        acced.put(1, 1);

        Map<Integer, Integer> combined = new HashMap<>();
        combined.put(1, 3);

        validateOpWithoutDeduct(
                toMap(Entry::getKey, Entry::getValue, Integer::sum),
                identity(), entry(1, 1), entry(1, 2),
                acced, combined, combined);
    }

    @Test
    public void when_mappingWithoutDeduct() {
        validateOpWithoutDeduct(
                mapping(entryValue(), maxBy(naturalOrder())),
                identity(),
                entry("a", 1),
                entry("b", 2),
                new MutableReference<>(1),
                new MutableReference<>(2),
                2
        );
    }

    @Test
    public void when_mappingWithDeduct() {
        validateOp(
                mapping(entryValue(), summingLong(i -> i)),
                identity(),
                entry("a", 1L),
                entry("b", 2L),
                new LongAccumulator(1),
                new LongAccumulator(3),
                3L
        );
    }

    @Test
    public void when_mappingToNull_then_doNotAggregate() {
        validateOp(
                mapping(entryValue(), summingLong(i -> i)),
                identity(),
                entry("a", null),
                entry("b", 2L),
                new LongAccumulator(0),
                new LongAccumulator(2),
                2L
        );
    }

    @Test
    public void when_filtering() {
        validateOp(
                filtering(i -> i > 1L, summingLong(i -> i)),
                identity(),
                1L,
                2L,
                new LongAccumulator(0),
                new LongAccumulator(2),
                2L
        );
    }

    @Test
    public void when_flatMapping() {
        validateOp(
                flatMapping(i -> Traversers.traverseItems(i + 10, i + 20), summingLong(i -> i)),
                identity(),
                1L,
                2L,
                new LongAccumulator(32), // 11 + 21
                new LongAccumulator(66), // 11 + 21 + 12 + 22
                66L
        );
    }

    @Test
    public void when_concatenating_withoutDelimiter() {
        validateOpWithoutDeduct(
                concatenating(),
                StringBuilder::toString,
                "A",
                "B",
                "A",
                "AB",
                "AB"
        );
    }

    @Test
    public void when_concatenating_withDelimiter() {
        validateOpWithoutDeduct(
                concatenating(","),
                StringBuilder::toString,
                "A",
                "B",
                "A",
                "A,B",
                "A,B"
        );
    }

    @Test
    public void when_concatenating_withDelimiterPrefixSuffix() {
        validateOpWithoutDeduct(
                concatenating(",", "(", ")"),
                StringBuilder::toString,
                "A",
                "B",
                "(A",
                "(A,B",
                "(A,B)"
        );
    }

    @Test
    public void when_concatenatingEmptyItems_withDelimiterPrefixSuffix() {
        validateOpWithoutDeduct(
                concatenating(",", "(", ")"),
                StringBuilder::toString,
                "A",
                "",
                "(A",
                "(A",
                "(A)"
        );
        validateOpWithoutDeduct(
                concatenating(",", "(", ")"),
                StringBuilder::toString,
                "",
                "B",
                "(",
                "(B",
                "(B)"
        );
        validateOpWithoutDeduct(
                concatenating(",", "(", ")"),
                StringBuilder::toString,
                "",
                "",
                "(",
                "(",
                "()"
        );
    }

    @Test
    public void when_groupingBy_withDifferentKey() {
        Entry<String, Integer> entryA = entry("a", 1);
        Entry<String, Integer> entryB = entry("b", 1);
        validateOpWithoutDeduct(
                groupingBy(entryKey()),
                identity(),
                entryA,
                entryB,
                asMap("a", singletonList(entryA)),
                asMap("a", singletonList(entryA), "b", singletonList(entryB)),
                asMap("a", singletonList(entryA), "b", singletonList(entryB))
        );
    }

    @Test
    public void when_groupingBy_withSameKey() {
        Entry<String, Integer> entryA = entry("a", 1);
        validateOpWithoutDeduct(
                groupingBy(entryKey()),
                identity(),
                entryA,
                entryA,
                asMap("a", singletonList(entryA)),
                asMap("a", asList(entryA, entryA)),
                asMap("a", asList(entryA, entryA))
        );
    }

    @Test
    public void when_groupingBy_withDownstreamOperation() {
        Entry<String, Integer> entryA = entry("a", 1);
        validateOpWithoutDeduct(
                groupingBy(entryKey(), counting()),
                identity(),
                entryA,
                entryA,
                asMap("a", longAcc(1)),
                asMap("a", longAcc(2)),
                asMap("a", 2L)
        );
    }

    @Test
    public void when_groupingBy_withDownstreamOperationAndMapSupplier() {
        Entry<String, Integer> entryA = entry("a", 1);
        Entry<String, Integer> entryB = entry("b", 1);
        validateOpWithoutDeduct(
                groupingBy(entryKey(), TreeMap::new, counting()),
                a -> {
                    assertThat(a, instanceOf(TreeMap.class));
                    return a;
                },
                entryB,
                entryA,
                asMap("b", longAcc(1)),
                asMap("a", longAcc(1), "b", longAcc(1)),
                asMap("a", 1L, "b", 1L)
        );
    }

    @Test
    public void when_pickAny() {
        validateOp(pickAny(), PickAnyAccumulator::get, 1, 2, 1, 1, 1);
    }

    @Test
    public void when_pickAny_noInput_then_nullResult() {
        // Given
        AggregateOperation1<Object, PickAnyAccumulator<Object>, Object> aggrOp = pickAny();

        // When
        Object result = aggrOp.finishFn().apply(aggrOp.createFn().get());

        // Then
        assertNull(result);
    }

    @Test
    public void when_sorting() {
        validateOpWithoutDeduct(
                sorting(naturalOrder()),
                identity(),
                2,
                1,
                singletonList(2),
                asList(2, 1),
                asList(1, 2)
        );
    }

    @Test
    public void when_aggregateOpAsCollector() {
        collectAndVerify(IntStream.range(0, 1000));
    }

    @Test
    public void when_aggregateOpAsParallelCollector() {
        collectAndVerify(IntStream.range(0, 1000).parallel());
    }

    private void collectAndVerify(IntStream stream) {
        AggregateOperation1<Integer, LongLongAccumulator, Double> averageOp = averagingLong(i -> i);
        AggregateOperation1<Integer, MutableReference<Integer>, Integer> maxOp = maxBy(naturalOrder());

        Tuple2<Double, Integer> result = stream
                .boxed()
                .collect(AggregateOperations.toCollector(allOf(averageOp, maxOp)));

        assertEquals((Double) 499.5d, result.f0());
        assertEquals((Integer) 999, result.f1());
    }

    private static <T, A, X, R> void validateOp(
            AggregateOperation1<T, A, R> op,
            Function<A, X> getAccValFn,
            T item1,
            T item2,
            X expectAcced1,
            X expectCombined,
            R expectFinished
    ) {
        // Given
        BiConsumer<? super A, ? super A> deductFn = op.deductFn();
        BiConsumerEx<? super A, ? super A> combineFn = op.combineFn();
        assertNotNull("combineFn", combineFn);
        assertNotNull("deductFn", deductFn);

        // When
        A acc1 = op.createFn().get();
        op.accumulateFn().accept(acc1, item1);

        A acc2 = op.createFn().get();
        op.accumulateFn().accept(acc2, item2);

        // Checks must be made early because combine/deduct
        // are allowed to be destructive ops

        // Then
        assertEqualsOrArrayEquals("accumulated", expectAcced1, getAccValFn.apply(acc1));

        R acc1Exported = op.exportFn().apply(acc1);
        byte[] acc1ExportedSerialized = serialize(acc1Exported);

        // When
        combineFn.accept(acc1, acc2);
        // Then
        assertEqualsOrArrayEquals("combined", expectCombined, getAccValFn.apply(acc1));

        // When - acc1 was mutated
        // Then - it's exported version must stay unchanged
        assertArrayEquals(acc1ExportedSerialized, serialize(acc1Exported));

        // When
        R exported = op.exportFn().apply(acc1);
        // Then
        assertEquals("exported", expectFinished, exported);
        assertNotSame(exported, acc1);

        // When
        R finished = op.finishFn().apply(acc1);
        // Then
        assertEqualsOrArrayEquals("finished", expectFinished, finished);

        // When
        deductFn.accept(acc1, acc2);
        // Then
        assertEqualsOrArrayEquals("deducted", expectAcced1, getAccValFn.apply(acc1));

        // When - accumulate both items into single accumulator
        acc1 = op.createFn().get();
        op.accumulateFn().accept(acc1, item1);
        op.accumulateFn().accept(acc1, item2);
        // Then
        assertEqualsOrArrayEquals("accumulated both", expectCombined, getAccValFn.apply(acc1));
    }

    private static void assertEqualsOrArrayEquals(String msg, Object expected, Object actual) {
        if (expected instanceof Object[]) {
            assertArrayEquals(msg, (Object[]) expected, (Object[]) actual);
        } else {
            assertEquals(msg, expected, actual);
        }
    }

    private static <T, A, X, R> void validateOpWithoutDeduct(
            AggregateOperation1<? super T, A, ? extends R> op,
            Function<? super A, ? extends X> getAccValFn,
            T itemLeft,
            T itemRight,
            X expectAccedLeft,
            X expectCombined,
            R expectFinished
    ) {
        validateOpWithoutDeduct(
                op, getAccValFn, singleton(itemLeft), singleton(itemRight), expectAccedLeft, expectCombined, expectFinished
        );
    }

    private static <T, A, X, R> void validateOpWithoutDeduct(
            AggregateOperation1<? super T, A, ? extends R> op,
            Function<? super A, ? extends X> getAccValFn,
            Iterable<T> itemsLeft,
            Iterable<T> itemsRight,
            X expectAccedLeft,
            X expectCombined,
            R expectFinished
    ) {
        // Given
        assertNull("deductFn", op.deductFn());
        BiConsumerEx<? super A, ? super A> combineFn = op.combineFn();
        assertNotNull("combineFn", combineFn);

        // When
        A acc1 = op.createFn().get();
        itemsLeft.forEach(item -> op.accumulateFn().accept(acc1, item));

        A acc2 = op.createFn().get();
        itemsRight.forEach(item -> op.accumulateFn().accept(acc2, item));

        // Checks must be made early because combine/deduct
        // are allowed to be destructive ops

        // Then
        assertEquals("accumulated", expectAccedLeft, getAccValFn.apply(acc1));

        R acc1Exported = op.exportFn().apply(acc1);
        byte[] acc1ExportedSerialized = serialize(acc1Exported);

        // When
        combineFn.accept(acc1, acc2);
        // Then
        assertEquals("combined", expectCombined, getAccValFn.apply(acc1));

        // When - acc1 was mutated
        // Then - it's exported version must stay unchanged
        assertArrayEquals(acc1ExportedSerialized, serialize(acc1Exported));

        // When
        R exported = op.exportFn().apply(acc1);
        // Then
        assertEquals("exported", expectFinished, exported);
        assertNotSame(exported, acc1);

        // When
        R finished = op.finishFn().apply(acc1);
        // Then
        assertEquals("finished", expectFinished, finished);

        // When - accumulate both sides into single accumulator
        A acc3 = op.createFn().get();
        itemsLeft.forEach(item -> op.accumulateFn().accept(acc3, item));
        itemsRight.forEach(item -> op.accumulateFn().accept(acc3, item));
        // Then
        assertEquals("accumulated", expectCombined, getAccValFn.apply(acc3));
    }

    private static byte[] serialize(Object o) {
        return SERIALIZATION_SERVICE.toBytes(o);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> Map<K, V> asMap(Object... entries) {
        Map<K, V> map = new HashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            map.put((K) entries[i], (V) entries[i + 1]);
        }
        return map;
    }

    private static LongAccumulator longAcc(long val) {
        return new LongAccumulator(val);
    }

    private static LongLongAccumulator longLongAcc(long val1, long val2) {
        return new LongLongAccumulator(val1, val2);
    }
}
