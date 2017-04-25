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

import com.hazelcast.jet.Accumulators.MutableLong;
import com.hazelcast.jet.Accumulators.MutableReference;
import com.hazelcast.jet.Distributed.BinaryOperator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.function.Function;

import static com.hazelcast.jet.windowing.WindowOperations.counting;
import static com.hazelcast.jet.windowing.WindowOperations.reducing;
import static com.hazelcast.jet.windowing.WindowOperations.summingToLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class WindowOperationsTest {
    @Test
    public void when_counting() {
        validateOp(counting(), MutableLong::getValue,
                new Object(), 1L, 2L, 1L);
    }

    @Test
    public void when_summingToLong() {
        validateOp(summingToLong(), MutableLong::getValue,
                1L, 1L, 2L, 1L);
    }

    @Test
    public void when_summingToLongWithMapper() {
        validateOp(summingToLong(x -> 1L), MutableLong::getValue,
                new Object(), 1L, 2L, 1L);
    }

    @Test
    public void when_reducing() {
        validateOp(reducing(0, Integer::valueOf, Integer::sum, (x, y) -> x - y),
                MutableReference::getValue,
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
        BinaryOperator<A> deductAccF = op.deductAccumulatorF();
        assertNotNull(deductAccF);

        // When

        A acc1 = op.createAccumulatorF().get();
        op.accumulateItemF().accept(acc1, item);

        A acc2 = op.createAccumulatorF().get();
        op.accumulateItemF().accept(acc2, item);

        // Checks must be made early because combine/deduct
        // are allowed to be destructive ops

        // Then
        assertEquals(expectAcced, getAccValF.apply(acc1));
        assertEquals(expectAcced, getAccValF.apply(acc2));

        // When
        A combined = op.combineAccumulatorsF().apply(acc1, acc2);
        // Then
        assertEquals(expectCombined, getAccValF.apply(combined));

        // When
        A deducted = deductAccF.apply(combined, acc2);
        // Then
        assertEquals(expectAcced, getAccValF.apply(combined));

        // When
        R finished = op.finishAccumulationF().apply(deducted);
        // Then
        assertEquals(expectFinished, finished);
    }
}
