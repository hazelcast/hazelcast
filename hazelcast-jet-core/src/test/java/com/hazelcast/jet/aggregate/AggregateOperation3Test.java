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

import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.datamodel.Tag.tag;
import static com.hazelcast.jet.datamodel.Tag.tag0;
import static com.hazelcast.jet.datamodel.Tag.tag1;
import static com.hazelcast.jet.datamodel.Tag.tag2;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
public class AggregateOperation3Test {

    @Test
    public void when_build_then_allPartsThere() {

        // Given
        DistributedSupplier<LongAccumulator> createFn = LongAccumulator::new;
        DistributedBiConsumer<LongAccumulator, Object> accFn0 = (acc, item) -> acc.addAllowingOverflow(1);
        DistributedBiConsumer<LongAccumulator, Object> accFn1 = (acc, item) -> acc.addAllowingOverflow(10);
        DistributedBiConsumer<LongAccumulator, Object> accFn2 = (acc, item) -> acc.addAllowingOverflow(100);
        DistributedBiConsumer<LongAccumulator, LongAccumulator> combineFn = LongAccumulator::addAllowingOverflow;
        DistributedBiConsumer<LongAccumulator, LongAccumulator> deductFn = LongAccumulator::subtractAllowingOverflow;
        DistributedFunction<LongAccumulator, Long> finishFn = LongAccumulator::get;

        // When
        AggregateOperation3<Object, Object, Object, LongAccumulator, Long> aggrOp = AggregateOperation
                .withCreate(createFn)
                .andAccumulate0(accFn0)
                .andAccumulate1(accFn1)
                .andAccumulate2(accFn2)
                .andCombine(combineFn)
                .andDeduct(deductFn)
                .andFinish(finishFn);

        // Then
        assertSame(createFn, aggrOp.createFn());
        assertSame(accFn0, aggrOp.accumulateFn0());
        assertSame(accFn0, aggrOp.accumulateFn(tag0()));
        assertSame(accFn1, aggrOp.accumulateFn1());
        assertSame(accFn1, aggrOp.accumulateFn(tag1()));
        assertSame(accFn2, aggrOp.accumulateFn2());
        assertSame(accFn2, aggrOp.accumulateFn(tag2()));
        assertSame(combineFn, aggrOp.combineFn());
        assertSame(deductFn, aggrOp.deductFn());
        assertSame(finishFn, aggrOp.finishFn());
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_askForNonexistentTag_then_exception() {
        // Given
        AggregateOperation3<Object, Object, Object, LongAccumulator, LongAccumulator> aggrOp = AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate0((x, y) -> { })
                .andAccumulate1((x, y) -> { })
                .andAccumulate2((x, y) -> { })
                .andIdentityFinish();

        // When - then exception
        aggrOp.accumulateFn(tag(3));
    }

    @Test
    public void when_withFinishFn_then_newFinishFn() {
        // Given
        AggregateOperation3<Object, Object, Object, LongAccumulator, LongAccumulator> aggrOp = AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate0((acc, item) -> { })
                .andAccumulate1((acc, item) -> { })
                .andAccumulate2((acc, item) -> { })
                .andIdentityFinish();

        // When
        DistributedFunction<LongAccumulator, String> newFinishFn = Object::toString;
        AggregateOperation3<Object, Object, Object, LongAccumulator, String> newAggrOp =
                aggrOp.withFinishFn(newFinishFn);

        // Then
        assertSame(newFinishFn, newAggrOp.finishFn());
    }

    @Test
    public void when_withCombiningAccumulateFn_then_accumulateFnCombines() {
        // Given
        AggregateOperation3<Object, Object, Object, LongAccumulator, LongAccumulator> aggrOp = AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate0((acc, item) -> acc.addAllowingOverflow(1))
                .andAccumulate1((acc, item) -> acc.addAllowingOverflow(10))
                .andAccumulate2((acc, item) -> acc.addAllowingOverflow(100))
                .andCombine(LongAccumulator::addAllowingOverflow)
                .andIdentityFinish();
        AggregateOperation1<LongAccumulator, LongAccumulator, LongAccumulator> combiningAggrOp =
                aggrOp.withCombiningAccumulateFn(wholeItem());
        DistributedBiConsumer<? super LongAccumulator, ? super LongAccumulator> accFn = combiningAggrOp.accumulateFn();
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
}
