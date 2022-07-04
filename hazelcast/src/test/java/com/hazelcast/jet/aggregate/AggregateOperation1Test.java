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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.datamodel.Tag.tag0;
import static com.hazelcast.jet.datamodel.Tag.tag1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AggregateOperation1Test {

    @Test
    public void when_build_then_allPartsThere() {

        // Given
        SupplierEx<LongAccumulator> createFn = LongAccumulator::new;
        BiConsumerEx<LongAccumulator, Object> accFn0 = (acc, item) -> acc.addAllowingOverflow(1);
        BiConsumerEx<LongAccumulator, LongAccumulator> combineFn = LongAccumulator::addAllowingOverflow;
        BiConsumerEx<LongAccumulator, LongAccumulator> deductFn = LongAccumulator::subtractAllowingOverflow;
        FunctionEx<LongAccumulator, Long> exportFn = acc -> 1L;
        FunctionEx<LongAccumulator, Long> finishFn = acc -> 2L;

        // When
        AggregateOperation1<Object, LongAccumulator, Long> aggrOp = AggregateOperation
                .withCreate(createFn)
                .andAccumulate(accFn0)
                .andCombine(combineFn)
                .andDeduct(deductFn)
                .andExport(exportFn)
                .andFinish(finishFn);

        // Then
        assertSame(createFn, aggrOp.createFn());
        assertSame(accFn0, aggrOp.accumulateFn());
        assertSame(accFn0, aggrOp.accumulateFn(tag0()));
        assertSame(combineFn, aggrOp.combineFn());
        assertSame(deductFn, aggrOp.deductFn());
        assertSame(exportFn, aggrOp.exportFn());
        assertSame(finishFn, aggrOp.finishFn());
    }

    @Test
    public void accumulate0_synonymFor_accumulate() {
        // Given
        BiConsumerEx<LongAccumulator, Object> accFn = (acc, item) -> acc.addAllowingOverflow(1);

        // When
        AggregateOperation1<Object, LongAccumulator, Long> aggrOp1 = AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate(accFn)
                .andExportFinish(LongAccumulator::get);
        AggregateOperation1<Object, LongAccumulator, Long> aggrOp2 = AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate0(accFn)
                .andExportFinish(LongAccumulator::get);

        // Then
        assertSame(accFn, aggrOp1.accumulateFn());
        assertSame(accFn, aggrOp2.accumulateFn());
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_askForNonexistentTag_then_exception() {
        // Given
        AggregateOperation1<Object, LongAccumulator, Long> aggrOp = AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate0((x, y) -> { })
                .andExportFinish(LongAccumulator::get);

        // When - then exception
        aggrOp.accumulateFn(tag1());
    }

    @Test
    public void when_withIdentityFinish() {
        // Given
        AggregateOperation1<Object, LongAccumulator, Long> aggrOp = AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate((acc, item) -> acc.addAllowingOverflow(1))
                .andExportFinish(LongAccumulator::get);

        // When
        AggregateOperation<LongAccumulator, LongAccumulator> newAggrOp = aggrOp.withIdentityFinish();

        // Then
        LongAccumulator acc = newAggrOp.createFn().get();
        assertSame(acc, newAggrOp.finishFn().apply(acc));
    }


    @Test
    public void when_withCombiningAccumulateFn_then_accumulateFnCombines() {
        // Given
        AggregateOperation1<Object, LongAccumulator, Long> aggrOp = AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate((acc, item) -> acc.addAllowingOverflow(1))
                .andCombine(LongAccumulator::addAllowingOverflow)
                .andExportFinish(LongAccumulator::get);
        AggregateOperation1<LongAccumulator, LongAccumulator, Long> combiningAggrOp =
                aggrOp.withCombiningAccumulateFn(wholeItem());
        BiConsumerEx<? super LongAccumulator, ? super LongAccumulator> accFn = combiningAggrOp.accumulateFn();
        LongAccumulator partialAcc1 = combiningAggrOp.createFn().get();
        LongAccumulator partialAcc2 = combiningAggrOp.createFn().get();
        LongAccumulator combinedAcc = combiningAggrOp.createFn().get();

        // When
        partialAcc1.set(2);
        partialAcc2.set(3);
        accFn.accept(combinedAcc, partialAcc1);
        accFn.accept(combinedAcc, partialAcc2);

        // Then
        assertEquals(5, combinedAcc.get());
    }

    @Test
    public void when_andThen_then_exportAndFinishChanged() {
        // Given
        AggregateOperation1<Long, LongAccumulator, Long> aggrOp = summingLong((Long x) -> x);

        // When
        AggregateOperation1<Long, LongAccumulator, Long> incAggrOp = aggrOp.andThen(a -> a + 1);

        // Then
        LongAccumulator acc = incAggrOp.createFn().get();
        incAggrOp.accumulateFn().accept(acc, 13L);
        long exported = incAggrOp.exportFn().apply(acc);
        long finished = incAggrOp.finishFn().apply(acc);
        assertEquals(14L, exported);
        assertEquals(14L, finished);
    }
}
