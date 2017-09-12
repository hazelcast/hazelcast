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

package com.hazelcast.jet.aggregate;

import com.hazelcast.jet.accumulator.DoubleAccumulator;
import com.hazelcast.jet.accumulator.LinTrendAccumulator;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.accumulator.LongDoubleAccumulator;
import com.hazelcast.jet.accumulator.LongLongAccumulator;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.function.DistributedFunctions;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.averagingDouble;
import static com.hazelcast.jet.aggregate.AggregateOperations.averagingLong;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.linearTrend;
import static com.hazelcast.jet.aggregate.AggregateOperations.mapping;
import static com.hazelcast.jet.aggregate.AggregateOperations.maxBy;
import static com.hazelcast.jet.aggregate.AggregateOperations.minBy;
import static com.hazelcast.jet.aggregate.AggregateOperations.reducing;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingDouble;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.aggregate.AggregateOperations.toList;
import static com.hazelcast.jet.aggregate.AggregateOperations.toMap;
import static com.hazelcast.jet.aggregate.AggregateOperations.toSet;
import static com.hazelcast.jet.function.DistributedComparator.naturalOrder;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class AggregateOperationsTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void when_counting() {
        validateOp(counting(), LongAccumulator::get,
                null, null, 1L, 2L, 2L);
    }

    @Test
    public void when_summingToLong() {
        validateOp(summingLong(Long::longValue), LongAccumulator::get,
                1L, 2L, 1L, 3L, 3L);
    }

    @Test
    public void when_summingToDouble() {
        validateOp(summingDouble(Double::doubleValue), DoubleAccumulator::get,
                0.5, 1.5, 0.5, 2.0, 2.0);
    }

    @Test
    public void when_averagingLong() {
        validateOp(averagingLong(Long::longValue), identity(),
                1L, 2L, new LongLongAccumulator(1, 1), new LongLongAccumulator(2, 3), 1.5);
    }

    @Test
    public void when_averagingDouble() {
        validateOp(averagingDouble(Double::doubleValue), identity(),
                1.5, 2.5, new LongDoubleAccumulator(1, 1.5), new LongDoubleAccumulator(2, 4.0), 2.0);
    }

    @Test
    public void when_maxBy() {
        validateOpWithoutDeduct(maxBy(naturalOrder()), MutableReference::get,
                10L, 11L, 10L, 11L, 11L);
    }

    @Test
    public void when_minBy() {
        validateOpWithoutDeduct(minBy(naturalOrder()), MutableReference::get,
                10L, 11L, 10L, 10L, 10L);
    }

    @Test
    public void when_allOf() {
        validateOp(
                allOf(counting(), summingLong(Long::longValue)),
                identity(), 10L, 11L,
                asList(new LongAccumulator(1L), new LongAccumulator(10L)),
                asList(new LongAccumulator(2L), new LongAccumulator(21L)),
                asList(2L, 21L)
        );
    }

    @Test
    public void when_allOfWithoutDeduct() {
        validateOpWithoutDeduct(
                allOf(counting(), maxBy(naturalOrder())),
                identity(), 10L, 11L,
                asList(new LongAccumulator(1), new MutableReference<>(10L)),
                asList(new LongAccumulator(2), new MutableReference<>(11L)),
                asList(2L, 11L)
        );
    }

    @Test
    public void when_linearTrend() {
        // Given
        AggregateOperation1<Entry<Long, Long>, LinTrendAccumulator, Double> op =
                linearTrend(Entry::getKey, Entry::getValue);
        Supplier<LinTrendAccumulator> newFn = op.createFn();
        BiConsumer<? super LinTrendAccumulator, ? super Entry<Long, Long>> accFn = op.accumulateFn();
        BiConsumer<? super LinTrendAccumulator, ? super LinTrendAccumulator> combineFn = op.combineFn();
        BiConsumer<? super LinTrendAccumulator, ? super LinTrendAccumulator> deductFn = op.deductFn();
        Function<? super LinTrendAccumulator, Double> finishFn = op.finishFn();
        assertNotNull(deductFn);

        // When
        LinTrendAccumulator a1 = newFn.get();
        accFn.accept(a1, entry(1L, 3L));
        accFn.accept(a1, entry(2L, 5L));
        assertEquals(2.0, finishFn.apply(a1), Double.MIN_VALUE);

        LinTrendAccumulator a2 = newFn.get();
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
        LinTrendAccumulator acc = newFn.get();
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
                toMap(entryKey(), entryValue()),
                identity(), entry(1, 1), entry(2, 2),
                acced, combined, combined);
    }

    @Test
    public void when_toMapDuplicateAccumulate_then_fail() {
        AggregateOperation1<Entry<Integer, Integer>, Map<Integer, Integer>, Map<Integer, Integer>> op =
                toMap(entryKey(), entryValue());

        Map<Integer, Integer> acc = op.createFn().get();
        op.accumulateFn().accept(acc, entry(1, 1));

        exception.expect(IllegalStateException.class);
        op.accumulateFn().accept(acc, entry(1, 2));
    }

    @Test
    public void when_toMapDuplicateCombine_then_fail() {
        AggregateOperation1<Entry<Integer, Integer>, Map<Integer, Integer>, Map<Integer, Integer>> op =
                toMap(entryKey(), entryValue());

        Map<Integer, Integer> acc1 = op.createFn().get();
        op.accumulateFn().accept(acc1, entry(1, 1));
        Map<Integer, Integer> acc2 = op.createFn().get();
        op.accumulateFn().accept(acc2, entry(1, 2));

        exception.expect(IllegalStateException.class);
        op.combineFn().accept(acc1, acc2);
    }

    @Test
    public void when_toMapWithMerge_then_merged() {
        Map<Integer, Integer> acced = new HashMap<>();
        acced.put(1, 1);

        Map<Integer, Integer> combined = new HashMap<>();
        combined.put(1, 3);

        validateOpWithoutDeduct(
                toMap(DistributedFunctions.<Integer, Integer>entryKey(), entryValue(), Integer::sum),
                identity(), entry(1, 1), entry(1, 2),
                acced, combined, combined);
    }

    @Test
    public void when_mappingWithoutDeduct() {
        validateOpWithoutDeduct(
                mapping((Entry<?, Integer> e) -> e.getValue(), maxBy(naturalOrder())),
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
                mapping((Entry<?, Long> e) -> e.getValue(), summingLong(i -> i)),
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
                mapping((Entry<?, Long> e) -> e.getValue(), summingLong(i -> i)),
                identity(),
                entry("a", null),
                entry("b", 2L),
                new LongAccumulator(0),
                new LongAccumulator(2),
                2L
        );
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
        BiConsumer<? super A, ? super A> deductAccFn = op.deductFn();
        assertNotNull(deductAccFn);

        // When
        A acc1 = op.createFn().get();
        op.accumulateFn().accept(acc1, item1);

        A acc2 = op.createFn().get();
        op.accumulateFn().accept(acc2, item2);

        // Checks must be made early because combine/deduct
        // are allowed to be destructive ops

        // Then
        assertEquals("accumulated", expectAcced1, getAccValFn.apply(acc1));

        // When
        op.combineFn().accept(acc1, acc2);
        // Then
        assertEquals("combined", expectCombined, getAccValFn.apply(acc1));

        // When
        R finished = op.finishFn().apply(acc1);
        // Then
        assertEquals("finished", expectFinished, finished);

        // When
        deductAccFn.accept(acc1, acc2);
        // Then
        assertEquals("deducted", expectAcced1, getAccValFn.apply(acc1));

        // When - accumulate both items into single accumulator
        acc1 = op.createFn().get();
        op.accumulateFn().accept(acc1, item1);
        op.accumulateFn().accept(acc1, item2);
        // Then
        assertEquals("accumulated", expectCombined, getAccValFn.apply(acc1));
    }

    private static <T, A, X, R> void validateOpWithoutDeduct(
            AggregateOperation1<T, A, R> op,
            Function<A, X> getAccValFn,
            T item1,
            T item2,
            X expectAcced,
            X expectCombined,
            R expectFinished
    ) {
        // Then
        assertNull(op.deductFn());

        // When
        A acc1 = op.createFn().get();
        op.accumulateFn().accept(acc1, item1);

        A acc2 = op.createFn().get();
        op.accumulateFn().accept(acc2, item2);

        // Checks must be made early because combine/deduct
        // are allowed to be destructive ops

        // Then
        assertEquals("accumulated", expectAcced, getAccValFn.apply(acc1));

        // When
        op.combineFn().accept(acc1, acc2);
        // Then
        assertEquals("combined", expectCombined, getAccValFn.apply(acc1));

        // When
        R finished = op.finishFn().apply(acc1);
        // Then
        assertEquals("finished", expectFinished, finished);

        // When - accumulate both items into single accumulator
        acc1 = op.createFn().get();
        op.accumulateFn().accept(acc1, item1);
        op.accumulateFn().accept(acc1, item2);
        // Then
        assertEquals("accumulated", expectCombined, getAccValFn.apply(acc1));
    }
}
