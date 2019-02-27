/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.SupplierEx;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class AggregateOperationImpl<A, R> implements AggregateOperation<A, R> {
    final BiConsumerEx<? super A, ?>[] accumulateFns;
    private final SupplierEx<A> createFn;
    private final BiConsumerEx<? super A, ? super A> combineFn;
    private final BiConsumerEx<? super A, ? super A> deductFn;
    private final FunctionEx<? super A, ? extends R> exportFn;
    private final FunctionEx<? super A, ? extends R> finishFn;

    public AggregateOperationImpl(
            @Nonnull SupplierEx<A> createFn,
            @Nonnull BiConsumerEx<? super A, ?>[] accumulateFns,
            @Nullable BiConsumerEx<? super A, ? super A> combineFn,
            @Nullable BiConsumerEx<? super A, ? super A> deductFn,
            @Nonnull FunctionEx<? super A, ? extends R> exportFn,
            @Nonnull FunctionEx<? super A, ? extends R> finishFn
    ) {
        for (Object f : accumulateFns) {
            checkNotNull(f, "accumulateFns array contains a null slot");
        }
        this.createFn = createFn;
        this.accumulateFns = accumulateFns.clone();
        this.combineFn = combineFn;
        this.deductFn = deductFn;
        this.exportFn = exportFn;
        this.finishFn = finishFn;
    }

    @Override
    public int arity() {
        return accumulateFns.length;
    }

    @Nonnull
    public SupplierEx<A> createFn() {
        return createFn;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> BiConsumerEx<? super A, ? super T> accumulateFn(int index) {
        if (index >= accumulateFns.length) {
            throw new IllegalArgumentException("This AggregateOperation has " + accumulateFns.length
                    + " accumulating functions, but was asked for function at index " + index);
        }
        return (BiConsumerEx<? super A, T>) accumulateFns[index];
    }

    @Nullable
    public BiConsumerEx<? super A, ? super A> combineFn() {
        return combineFn;
    }

    @Nullable
    public BiConsumerEx<? super A, ? super A> deductFn() {
        return deductFn;
    }

    @Nonnull
    public FunctionEx<? super A, ? extends R> exportFn() {
        return exportFn;
    }

    @Nonnull
    public FunctionEx<? super A, ? extends R> finishFn() {
        return finishFn;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public AggregateOperation<A, R> withAccumulateFns(BiConsumerEx... accumulateFns) {
        return new AggregateOperationImpl<>(
                createFn(), accumulateFns, combineFn(), deductFn(), exportFn(), finishFn());
    }

    @Nonnull @Override
    public AggregateOperation<A, A> withIdentityFinish() {
        checkSerializable(finishFn, "finishFn");
        return new AggregateOperationImpl<>(
                createFn(), accumulateFns, combineFn(), deductFn(),
                unsupportedExportFn(), FunctionEx.identity());
    }

    @Nonnull
    @Override
    public <R_NEW> AggregateOperation<A, R_NEW> andThen(FunctionEx<? super R, ? extends R_NEW> thenFn) {
        return new AggregateOperationImpl<>(
                createFn(), accumulateFns, combineFn(), deductFn(),
                exportFn().andThen(thenFn), finishFn().andThen(thenFn)
        );
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    static <A> BiConsumerEx<? super A, ?>[] accumulateFns(BiConsumerEx... accFns) {
        return (BiConsumerEx<? super A, ?>[]) accFns;
    }

    FunctionEx<? super A, ? extends A> unsupportedExportFn() {
        return x -> {
            throw new UnsupportedOperationException(
                    "Can't use exportFn on an aggregate operation with identity finishFn");
        };
    }
}
