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
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.aggregate.AggregateOperations.coAggregateOperationBuilder;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.datamodel.Tag.tag0;
import static com.hazelcast.jet.datamodel.Tag.tag1;
import static com.hazelcast.jet.datamodel.Tag.tag2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AggregateOperationTest {

    @Test
    public void when_build_then_allPartsThere() {

        // Given
        SupplierEx<LongAccumulator> createFn = LongAccumulator::new;
        BiConsumerEx<LongAccumulator, Object> accFn0 = (acc, item) -> acc.add(1);
        BiConsumerEx<LongAccumulator, Object> accFn1 = (acc, item) -> acc.add(10);
        BiConsumerEx<LongAccumulator, LongAccumulator> combineFn = LongAccumulator::add;
        BiConsumerEx<LongAccumulator, LongAccumulator> deductFn = LongAccumulator::subtract;
        FunctionEx<LongAccumulator, Long> finishFn = LongAccumulator::get;

        // When
        AggregateOperation<LongAccumulator, Long> aggrOp = AggregateOperation
                .withCreate(createFn)
                .andAccumulate(tag0(), accFn0)
                .andAccumulate(tag1(), accFn1)
                .andCombine(combineFn)
                .andDeduct(deductFn)
                .andExportFinish(finishFn);

        // Then
        assertSame(createFn, aggrOp.createFn());
        assertSame(accFn0, aggrOp.accumulateFn(tag0()));
        assertSame(accFn1, aggrOp.accumulateFn(tag1()));
        assertSame(combineFn, aggrOp.combineFn());
        assertSame(deductFn, aggrOp.deductFn());
        assertSame(finishFn, aggrOp.finishFn());
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_askForNonexistentTag_then_exception() {
        // Given
        AggregateOperation<LongAccumulator, Long> aggrOp = AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate(tag0(), (acc, item) -> acc.add(1))
                .andAccumulate(tag1(), (acc, item) -> acc.add(10))
                .andExportFinish(LongAccumulator::get);

        // When - then exception
        aggrOp.accumulateFn(tag2());
    }

    @Test
    public void when_withIdentityFinish() {
        // Given
        AggregateOperation<LongAccumulator, Long> aggrOp = AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate(tag0(), (x, y) -> { })
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
        AggregateOperation<LongAccumulator, Long> aggrOp = AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate(tag0(), (acc, item) -> acc.add(1))
                .andAccumulate(tag1(), (acc, item) -> acc.add(10))
                .andCombine(LongAccumulator::add)
                .andExportFinish(LongAccumulator::get);
        AggregateOperation1<LongAccumulator, LongAccumulator, Long> combiningAggrOp =
                aggrOp.withCombiningAccumulateFn(wholeItem());
        BiConsumerEx<? super LongAccumulator, ? super Object> accFn = combiningAggrOp.accumulateFn(tag0());
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
        CoAggregateOperationBuilder b = coAggregateOperationBuilder();
        b.add(tag0(), summingLong((Long x) -> x));
        Tag<Long> outTag1 = b.add(tag1(), summingLong((Long x) -> x));
        AggregateOperation<Object[], Long> aggrOp = b.build(ibt -> ibt.get(outTag1));

        // When
        AggregateOperation<Object[], Long> incAggrOp = aggrOp.andThen(a -> a + 1);

        // Then
        Object[] acc = incAggrOp.createFn().get();
        incAggrOp.accumulateFn(tag1()).accept(acc, 13L);
        long exported = incAggrOp.exportFn().apply(acc);
        long finished = incAggrOp.finishFn().apply(acc);
        assertEquals(14L, exported);
        assertEquals(14L, finished);
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_duplicateTag_then_exception() {
        AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate(tag0(), (acc, item) -> acc.add(1))
                .andAccumulate(tag0(), (acc, item) -> acc.add(10));
    }

    @Test(expected = IllegalStateException.class)
    public void when_tagsNonContiguous_then_exception() {
        AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate(tag0(), (acc, item) -> acc.add(1))
                .andAccumulate(tag2(), (acc, item) -> acc.add(10))
                .andExportFinish(LongAccumulator::get);
    }
}
