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
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class AggregateOperationImpl<A, R> implements AggregateOperation<A, R> {
    final DistributedBiConsumer<? super A, ?>[] accumulateFns;
    private final DistributedSupplier<A> createFn;
    private final DistributedBiConsumer<? super A, ? super A> combineFn;
    private final DistributedBiConsumer<? super A, ? super A> deductFn;
    private final DistributedFunction<? super A, ? extends R> exportFn;
    private final DistributedFunction<? super A, ? extends R> finishFn;

    public AggregateOperationImpl(
            @Nonnull DistributedSupplier<A> createFn,
            @Nonnull DistributedBiConsumer<? super A, ?>[] accumulateFns,
            @Nullable DistributedBiConsumer<? super A, ? super A> combineFn,
            @Nullable DistributedBiConsumer<? super A, ? super A> deductFn,
            @Nonnull DistributedFunction<? super A, ? extends R> exportFn,
            @Nonnull DistributedFunction<? super A, ? extends R> finishFn
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
    public DistributedSupplier<A> createFn() {
        return createFn;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> DistributedBiConsumer<? super A, ? super T> accumulateFn(int index) {
        if (index >= accumulateFns.length) {
            throw new IllegalArgumentException("This AggregateOperation has " + accumulateFns.length
                    + " accumulating functions, but was asked for function at index " + index);
        }
        return (DistributedBiConsumer<? super A, T>) accumulateFns[index];
    }

    @Nullable
    public DistributedBiConsumer<? super A, ? super A> combineFn() {
        return combineFn;
    }

    @Nullable
    public DistributedBiConsumer<? super A, ? super A> deductFn() {
        return deductFn;
    }

    @Nonnull
    public DistributedFunction<? super A, ? extends R> exportFn() {
        return exportFn;
    }

    @Nonnull
    public DistributedFunction<? super A, ? extends R> finishFn() {
        return finishFn;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public AggregateOperation<A, R> withAccumulateFns(DistributedBiConsumer... accumulateFns) {
        return new AggregateOperationImpl<>(
                createFn(), accumulateFns, combineFn(), deductFn(), exportFn(), finishFn());
    }

    @Nonnull @Override
    public AggregateOperation<A, A> withIdentityFinish() {
        checkSerializable(finishFn, "finishFn");
        return new AggregateOperationImpl<>(
                createFn(), accumulateFns, combineFn(), deductFn(),
                unsupportedExportFn(), DistributedFunction.identity());
    }

    @Nonnull
    @Override
    public <R_NEW> AggregateOperation<A, R_NEW> andThen(DistributedFunction<? super R, ? extends R_NEW> thenFn) {
        return new AggregateOperationImpl<>(
                createFn(), accumulateFns, combineFn(), deductFn(),
                exportFn().andThen(thenFn), finishFn().andThen(thenFn)
        );
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    static <A> DistributedBiConsumer<? super A, ?>[] accumulateFns(DistributedBiConsumer... accFns) {
        return (DistributedBiConsumer<? super A, ?>[]) accFns;
    }

    DistributedFunction<? super A, ? extends A> unsupportedExportFn() {
        return x -> {
            throw new UnsupportedOperationException(
                    "Can't use exportFn on an aggregate operation with identity finishFn");
        };
    }
}
