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
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.datamodel.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.checkSerializable;


public class AggregateOperation3Impl<T0, T1, T2, A, R>
        extends AggregateOperationImpl<A, R>
        implements AggregateOperation3<T0, T1, T2, A, R> {

    public AggregateOperation3Impl() {
    }

    public AggregateOperation3Impl(
            @Nonnull SupplierEx<A> createFn,
            @Nonnull BiConsumerEx<? super A, ? super T0> accumulateFn0,
            @Nonnull BiConsumerEx<? super A, ? super T1> accumulateFn1,
            @Nonnull BiConsumerEx<? super A, ? super T2> accumulateFn2,
            @Nullable BiConsumerEx<? super A, ? super A> combineFn,
            @Nullable BiConsumerEx<? super A, ? super A> deductFn,
            @Nonnull FunctionEx<? super A, ? extends R> exportFn,
            @Nonnull FunctionEx<? super A, ? extends R> finishFn
    ) {
        super(createFn, accumulateFns(accumulateFn0, accumulateFn1, accumulateFn2),
                combineFn, deductFn, exportFn, finishFn);
    }

    private AggregateOperation3Impl(
            @Nonnull SupplierEx<A> createFn,
            @Nonnull BiConsumerEx<? super A, ?>[] accumulateFns,
            @Nullable BiConsumerEx<? super A, ? super A> combineFn,
            @Nullable BiConsumerEx<? super A, ? super A> deductFn,
            @Nonnull FunctionEx<? super A, ? extends R> exportFn,
            @Nonnull FunctionEx<? super A, ? extends R> finishFn
    ) {
        super(createFn, accumulateFns, combineFn, deductFn, exportFn, finishFn);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public BiConsumerEx<? super A, ? super T0> accumulateFn0() {
        return (BiConsumerEx<? super A, ? super T0>) accumulateFns[0];
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public BiConsumerEx<? super A, ? super T1> accumulateFn1() {
        return (BiConsumerEx<? super A, ? super T1>) accumulateFns[1];
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public BiConsumerEx<? super A, ? super T2> accumulateFn2() {
        return (BiConsumerEx<? super A, ? super T2>) accumulateFns[2];
    }

    @Nonnull @Override
    public <T0_NEW> AggregateOperation3<T0_NEW, T1, T2, A, R> withAccumulateFn0(
            @Nonnull BiConsumerEx<? super A, ? super T0_NEW> accumulateFn0
    ) {
        checkSerializable(accumulateFn0, "accumulateFn0");
        return new AggregateOperation3Impl<>(
                createFn(), accumulateFn0, accumulateFn1(), accumulateFn2(),
                combineFn(), deductFn(), exportFn(), finishFn());
    }

    @Nonnull @Override
    public <T1_NEW> AggregateOperation3<T0, T1_NEW, T2, A, R> withAccumulateFn1(
            @Nonnull BiConsumerEx<? super A, ? super T1_NEW> accumulateFn1
    ) {
        checkSerializable(accumulateFn1, "accumulateFn1");
        return new AggregateOperation3Impl<>(
                createFn(), accumulateFn0(), accumulateFn1, accumulateFn2(),
                combineFn(), deductFn(), exportFn(), finishFn());
    }

    @Nonnull @Override
    public <T2_NEW> AggregateOperation3<T0, T1, T2_NEW, A, R> withAccumulateFn2(
            @Nonnull BiConsumerEx<? super A, ? super T2_NEW> accumulateFn2
    ) {
        checkSerializable(accumulateFn2, "accumulateFn2");
        return new AggregateOperation3Impl<>(
                createFn(), accumulateFn0(), accumulateFn1(), accumulateFn2,
                combineFn(), deductFn(), exportFn(), finishFn());
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> BiConsumerEx<? super A, T> accumulateFn(@Nonnull Tag<T> tag) {
        if (tag.index() > 2) {
            throw new IllegalArgumentException(
                    "AggregateOperation3 only recognizes tags with index 0, 1 and 2, but asked for " + tag.index());
        }
        return (BiConsumerEx<? super A, T>) accumulateFns[tag.index()];
    }

    @Nonnull @Override
    public AggregateOperation3<T0, T1, T2, A, A> withIdentityFinish() {
        return new AggregateOperation3Impl<>(
                createFn(), accumulateFns, combineFn(), deductFn(),
                unsupportedExportFn(), FunctionEx.identity());
    }

    @Nonnull @Override
    public <R_NEW> AggregateOperation3<T0, T1, T2, A, R_NEW> andThen(
            FunctionEx<? super R, ? extends R_NEW> thenFn
    ) {
        return new AggregateOperation3Impl<>(
                createFn(), accumulateFns, combineFn(), deductFn(),
                exportFn().andThen(thenFn), finishFn().andThen(thenFn)
        );
    }

    @Override
    public int getClassId() {
        return AggregateDataSerializerHook.AGGREGATE_OPERATION_3_IMPL;
    }
}
