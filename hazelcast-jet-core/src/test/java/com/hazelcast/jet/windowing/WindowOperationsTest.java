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

package com.hazelcast.jet.windowing;

import com.hazelcast.jet.accumulator.DoubleAccumulator;
import com.hazelcast.jet.accumulator.LinTrendAccumulator;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.accumulator.LongDoubleAccumulator;
import com.hazelcast.jet.accumulator.LongLongAccumulator;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.function.DistributedComparator.naturalOrder;
import static com.hazelcast.jet.windowing.WindowOperations.allOf;
import static com.hazelcast.jet.windowing.WindowOperations.averagingDouble;
import static com.hazelcast.jet.windowing.WindowOperations.averagingLong;
import static com.hazelcast.jet.windowing.WindowOperations.counting;
import static com.hazelcast.jet.windowing.WindowOperations.linearTrend;
import static com.hazelcast.jet.windowing.WindowOperations.maxBy;
import static com.hazelcast.jet.windowing.WindowOperations.minBy;
import static com.hazelcast.jet.windowing.WindowOperations.reducing;
import static com.hazelcast.jet.windowing.WindowOperations.summingToDouble;
import static com.hazelcast.jet.windowing.WindowOperations.summingToLong;
import static java.util.function.Function.identity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WindowOperationsTest {
    @Test
    public void when_counting() {
        validateOp(counting(), LongAccumulator::get,
                null, null, 1L, 2L, 2L);
    }

    @Test
    public void when_summingToLong() {
        validateOp(summingToLong(Long::longValue), LongAccumulator::get,
                1L, 2L, 1L, 3L, 3L);
    }

    @Test
    public void when_summingToDouble() {
        validateOp(summingToDouble(Double::doubleValue), DoubleAccumulator::get,
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
                allOf(counting(), summingToLong(Long::longValue)),
                identity(), 10L, 11L,
                Arrays.asList(new LongAccumulator(1L), new LongAccumulator(10L)),
                Arrays.asList(new LongAccumulator(2L), new LongAccumulator(21L)),
                Arrays.asList(2L, 21L)
        );
    }

    @Test
    public void when_allOfWithoutDeduct() {
        validateOpWithoutDeduct(
                allOf(counting(), maxBy(naturalOrder())),
                identity(), 10L, 11L,
                Arrays.asList(new LongAccumulator(1), new MutableReference<>(10L)),
                Arrays.asList(new LongAccumulator(2), new MutableReference<>(11L)),
                Arrays.asList(2L, 11L)
        );
    }

    @Test
    public void when_linearTrend() {
        // Given
        WindowOperation<Entry<Long, Long>, LinTrendAccumulator, Double> op = linearTrend(Entry::getKey, Entry::getValue);
        DistributedSupplier<LinTrendAccumulator> newF = op.createAccumulatorF();
        BiFunction<LinTrendAccumulator, Entry<Long, Long>, LinTrendAccumulator> accF = op.accumulateItemF();
        DistributedBinaryOperator<LinTrendAccumulator> combineF = op.combineAccumulatorsF();
        DistributedBinaryOperator<LinTrendAccumulator> deductF = op.deductAccumulatorF();
        DistributedFunction<LinTrendAccumulator, Double> finishF = op.finishAccumulationF();
        assertNotNull(deductF);

        // When
        LinTrendAccumulator a1 = newF.get();
        accF.apply(a1, entry(1L, 3L));
        accF.apply(a1, entry(2L, 5L));
        assertEquals(2.0, finishF.apply(a1), Double.MIN_VALUE);

        LinTrendAccumulator a2 = newF.get();
        accF.apply(a2, entry(5L, 11L));
        accF.apply(a2, entry(6L, 13L));
        assertEquals(2.0, finishF.apply(a2), Double.MIN_VALUE);

        LinTrendAccumulator combined = combineF.apply(a1, a2);
        assertEquals(2.0, finishF.apply(combined), Double.MIN_VALUE);

        LinTrendAccumulator deducted = deductF.apply(combined, a2);
        assertEquals(2.0, finishF.apply(combined), Double.MIN_VALUE);

        Double result = finishF.apply(deducted);
        assertEquals(Double.valueOf(2), result);

        // When
        a1 = newF.get();
        // Then
        assertTrue("NaN expected if nothing accumulated", Double.isNaN(finishF.apply(a1)));

        // When
        a1 = accF.apply(a1, entry(2L, 1L));
        // Then
        assertTrue("NaN expected if just single point accumulated", Double.isNaN(finishF.apply(a1)));

        // When
        a1 = accF.apply(a1, entry(2L, 1L));
        // Then
        assertTrue("NaN expected if all data points are equal", Double.isNaN(finishF.apply(a1)));

        // When
        a1 = accF.apply(a1, entry(2L, 2L));
        // Then
        assertTrue("NaN expected if all data points have same x value", Double.isNaN(finishF.apply(a1)));
    }

    @Test
    public void when_reducing() {
        validateOp(reducing(0, Integer::intValue, Integer::sum, (x, y) -> x - y),
                MutableReference::get,
                1, 2, 1, 3, 3);
    }

    private static <T, A, X, R> void validateOp(
            WindowOperation<T, A, R> op,
            Function<A, X> getAccValF,
            T item1,
            T item2,
            X expectAcced1,
            X expectCombined,
            R expectFinished
    ) {
        // Given
        DistributedBinaryOperator<A> deductAccF = op.deductAccumulatorF();
        assertNotNull(deductAccF);

        // When
        A acc1 = op.createAccumulatorF().get();
        acc1 = op.accumulateItemF().apply(acc1, item1);

        A acc2 = op.createAccumulatorF().get();
        acc2 = op.accumulateItemF().apply(acc2, item2);

        // Checks must be made early because combine/deduct
        // are allowed to be destructive ops

        // Then
        assertEquals("accumulated", expectAcced1, getAccValF.apply(acc1));

        // When
        A combined = op.combineAccumulatorsF().apply(acc1, acc2);
        // Then
        assertEquals("combined", expectCombined, getAccValF.apply(combined));

        // When
        R finished = op.finishAccumulationF().apply(combined);
        // Then
        assertEquals("finished", expectFinished, finished);

        // When
        combined = deductAccF.apply(combined, acc2);
        // Then
        assertEquals("deducted", expectAcced1, getAccValF.apply(combined));

        // When - accumulate both items into single accumulator
        acc1 = op.createAccumulatorF().get();
        acc1 = op.accumulateItemF().apply(acc1, item1);
        acc1 = op.accumulateItemF().apply(acc1, item2);
        // Then
        assertEquals("accumulated", expectCombined, getAccValF.apply(acc1));
    }

    private static <T, A, X, R> void validateOpWithoutDeduct(
            WindowOperation<T, A, R> op,
            Function<A, X> getAccValF,
            T item1,
            T item2,
            X expectAcced,
            X expectCombined,
            R expectFinished
    ) {
        // Then
        assertNull(op.deductAccumulatorF());

        // When
        A acc1 = op.createAccumulatorF().get();
        acc1 = op.accumulateItemF().apply(acc1, item1);

        A acc2 = op.createAccumulatorF().get();
        acc2 = op.accumulateItemF().apply(acc2, item2);

        // Checks must be made early because combine/deduct
        // are allowed to be destructive ops

        // Then
        assertEquals("accumulated", expectAcced, getAccValF.apply(acc1));

        // When
        A combined = op.combineAccumulatorsF().apply(acc1, acc2);
        // Then
        assertEquals("combined", expectCombined, getAccValF.apply(combined));

        // When
        R finished = op.finishAccumulationF().apply(combined);
        // Then
        assertEquals("finished", expectFinished, finished);

        // When - accumulate both items into single accumulator
        acc1 = op.createAccumulatorF().get();
        acc1 = op.accumulateItemF().apply(acc1, item1);
        acc1 = op.accumulateItemF().apply(acc1, item2);
        // Then
        assertEquals("accumulated", expectCombined, getAccValF.apply(acc1));
    }
}
