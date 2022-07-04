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

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.JetDataSerializerHook;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.checkSerializable;


public class AggregateOperation1Impl<T0, A, R>
        extends AggregateOperationImpl<A, R>
        implements AggregateOperation1<T0, A, R> {

    public AggregateOperation1Impl() {
    }

    public AggregateOperation1Impl(
            @Nonnull SupplierEx<A> createFn,
            @Nonnull BiConsumerEx<? super A, ? super T0> accumulateFn,
            @Nullable BiConsumerEx<? super A, ? super A> combineFn,
            @Nullable BiConsumerEx<? super A, ? super A> deductFn,
            @Nonnull FunctionEx<? super A, ? extends R> exportFn,
            @Nonnull FunctionEx<? super A, ? extends R> finishFn
    ) {
        super(createFn, accumulateFns(accumulateFn), combineFn, deductFn, exportFn, finishFn);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public BiConsumerEx<? super A, ? super T0> accumulateFn() {
        return (BiConsumerEx<? super A, ? super T0>) accumulateFns[0];
    }

    @Nonnull @Override
    public <NEW_T> AggregateOperation1<NEW_T, A, R> withAccumulateFn(
            BiConsumerEx<? super A, ? super NEW_T> accumulateFn
    ) {
        checkSerializable(accumulateFn, "accumulateFn");
        return new AggregateOperation1Impl<>(
                createFn(), accumulateFn, combineFn(), deductFn(), exportFn(), finishFn());
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> BiConsumerEx<? super A, ? super T> accumulateFn(@Nonnull Tag<T> tag) {
        if (tag.index() != 0) {
            throw new IllegalArgumentException("AggregateOperation1 recognizes only tag with index 0, but asked for "
                    + tag.index());
        }
        return (BiConsumerEx<? super A, ? super T>) accumulateFns[0];
    }

    @Nonnull @Override
    public AggregateOperation1<T0, A, A> withIdentityFinish() {
        return new AggregateOperation1Impl<>(
                createFn(), accumulateFn(), combineFn(), deductFn(),
                unsupportedExportFn(), FunctionEx.identity());
    }

    @Nonnull @Override
    public <R_NEW> AggregateOperation1<T0, A, R_NEW> andThen(FunctionEx<? super R, ? extends R_NEW> thenFn) {
        return new AggregateOperation1Impl<>(
                createFn(), accumulateFn(), combineFn(), deductFn(),
                exportFn().andThen(thenFn), finishFn().andThen(thenFn)
        );
    }

    @Override
    public int getClassId() {
        return AggregateDataSerializerHook.AGGREGATE_OPERATION_1_IMPL;
    }

    public static class AggregateCombiningAccumulate<A, T> implements IdentifiedDataSerializable, BiConsumerEx<A, T> {
        private FunctionEx<T, A> getAccFn;
        private BiConsumerEx<? super A, ? super A> combineFn;

        public AggregateCombiningAccumulate() {
        }

        public AggregateCombiningAccumulate(FunctionEx<T, A> getAccFn, BiConsumerEx<? super A, ? super A> combineFn) {
            this.getAccFn = getAccFn;
            this.combineFn = combineFn;
        }

        @Override
        public void acceptEx(A acc, T item) throws Exception {
            combineFn.accept(acc, getAccFn.apply(item));
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(getAccFn);
            out.writeObject(combineFn);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            getAccFn = in.readObject();
            combineFn = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetDataSerializerHook.AGGREGATE_COMBINING_ACCUMULATE;
        }
    }
}
