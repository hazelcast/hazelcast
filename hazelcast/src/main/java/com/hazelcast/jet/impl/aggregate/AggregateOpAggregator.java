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

package com.hazelcast.jet.impl.aggregate;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class AggregateOpAggregator<T, A, R> implements Aggregator<T, R>, IdentifiedDataSerializable {

    private AggregateOperation1<? super T, A, ? extends R> aggrOp;
    private A accumulator;

    public AggregateOpAggregator() {
    }

    public AggregateOpAggregator(AggregateOperation1<? super T, A, ? extends R> aggrOp) {
        requireNonNull(aggrOp.combineFn(),
                "The supplied AggregateOperation doesn't have the combineFn, which is required for an Aggregator");
        this.aggrOp = aggrOp;
        this.accumulator = aggrOp.createFn().get();
    }

    @Override
    public void accumulate(T input) {
        aggrOp.accumulateFn().accept(accumulator, input);
    }

    @Override
    public void combine(Aggregator aggregator) {
        @SuppressWarnings("unchecked")
        AggregateOpAggregator<? super T, A, ? extends R> other = (AggregateOpAggregator) aggregator;
        aggrOp.combineFn().accept(accumulator, other.accumulator);
    }

    @Override
    public R aggregate() {
        return aggrOp.finishFn().apply(accumulator);
    }

    @Override
    public int getFactoryId() {
        return AggregateDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return AggregateDataSerializerHook.AGGREGATE_OP_AGGREGATOR;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(aggrOp);
        out.writeObject(accumulator);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        aggrOp = in.readObject();
        accumulator = in.readObject();
    }
}
