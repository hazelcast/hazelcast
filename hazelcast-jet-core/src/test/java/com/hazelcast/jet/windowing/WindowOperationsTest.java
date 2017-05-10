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

import com.hazelcast.jet.accumulator.LinTrendAccumulator;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedOptional;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.stream.DistributedCollectors.maxBy;
import static com.hazelcast.jet.windowing.WindowOperation.fromCollector;
import static com.hazelcast.jet.windowing.WindowOperations.allOf;
import static com.hazelcast.jet.windowing.WindowOperations.counting;
import static com.hazelcast.jet.windowing.WindowOperations.linearTrend;
import static com.hazelcast.jet.windowing.WindowOperations.reducing;
import static com.hazelcast.jet.windowing.WindowOperations.summingToLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WindowOperationsTest {
    @Test
    public void when_counting() {
        validateOp(counting(), LongAccumulator::get,
                new Object(), 1L, 2L, 1L);
    }

    @Test
    public void when_summingToLong() {
        validateOp(summingToLong(), LongAccumulator::get,
                1L, 1L, 2L, 1L);
    }

    @Test
    public void when_summingToLongWithMapper() {
        validateOp(summingToLong(x -> 1L), LongAccumulator::get,
                new Object(), 1L, 2L, 1L);
    }

    @Test
    public void when_allOf() {
        validateOp(
                allOf(counting(), summingToLong()),
                Function.identity(),
                10L,
                Arrays.asList(new LongAccumulator(1L), new LongAccumulator(10L)),
                Arrays.asList(new LongAccumulator(2L), new LongAccumulator(20L)),
                Arrays.asList(1L, 10L)
        );
    }

    @Test
    public void when_allOfWithoutDeduct() {
        WindowOperation<Long, List<Object>, List<Object>> op = allOf(counting(),
                fromCollector(maxBy(DistributedComparator.<Long>naturalOrder())));

        // Then
        assertNull(op.deductAccumulatorF());

        // When
        List<Object> acc1 = op.createAccumulatorF().get();
        acc1 = op.accumulateItemF().apply(acc1, 10L);

        List<Object> acc2 = op.createAccumulatorF().get();
        acc2 = op.accumulateItemF().apply(acc2, 20L);

        // Checks must be made early because combine/deduct
        // are allowed to be destructive ops

        List<Object> combined = op.combineAccumulatorsF().apply(acc1, acc2);

        // When
        List<Object> finished = op.finishAccumulationF().apply(combined);
        // Then
        assertEquals("finished", Arrays.asList(2L, DistributedOptional.of(20L)), finished);
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
        assertEquals(2.0, a1.finish(), Double.MIN_VALUE);

        LinTrendAccumulator a2 = newF.get();
        accF.apply(a2, entry(5L, 11L));
        accF.apply(a2, entry(6L, 13L));
        assertEquals(2.0, a2.finish(), Double.MIN_VALUE);

        LinTrendAccumulator combined = combineF.apply(a1, a2);
        assertEquals(2.0, combined.finish(), Double.MIN_VALUE);

        LinTrendAccumulator deducted = deductF.apply(combined, a2);
        assertEquals(2.0, deducted.finish(), Double.MIN_VALUE);

        Double result = finishF.apply(deducted);
        assertEquals(Double.valueOf(2), result);
    }

    @Test
    public void when_reducing() {
        validateOp(reducing(0, Integer::valueOf, Integer::sum, (x, y) -> x - y),
                MutableReference::get,
                "1", 1, 2, 1);
    }

    private static <T, A, X, R> void validateOp(
            WindowOperation<T, A, R> op,
            Function<A, X> getAccValF,
            T item,
            X expectAcced,
            X expectCombined,
            R expectFinished
    ) {
        // Given
        DistributedBinaryOperator<A> deductAccF = op.deductAccumulatorF();
        assertNotNull(deductAccF);

        // When
        A acc1 = op.createAccumulatorF().get();
        acc1 = op.accumulateItemF().apply(acc1, item);

        A acc2 = op.createAccumulatorF().get();
        acc2 = op.accumulateItemF().apply(acc2, item);

        // Checks must be made early because combine/deduct
        // are allowed to be destructive ops

        // Then
        assertEquals("accumulated", expectAcced, getAccValF.apply(acc1));
        assertEquals("accumulated", expectAcced, getAccValF.apply(acc2));

        // When
        A combined = op.combineAccumulatorsF().apply(acc1, acc2);
        // Then
        assertEquals("combined", expectCombined, getAccValF.apply(combined));

        // When
        A deducted = deductAccF.apply(combined, acc2);
        // Then
        assertEquals("deducted", expectAcced, getAccValF.apply(combined));

        // When
        R finished = op.finishAccumulationF().apply(deducted);
        // Then
        assertEquals("finished", expectFinished, finished);
    }
}
