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

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.aggregate.AggregateOperation1Impl;
import com.hazelcast.jet.impl.aggregate.AggregateOperation2Impl;
import com.hazelcast.jet.impl.aggregate.AggregateOperation3Impl;
import com.hazelcast.jet.impl.aggregate.AggregateOperationImpl;
import com.hazelcast.jet.pipeline.datamodel.Tag;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

/**
 * Javadoc pending.
 */
public final class AggregateOperationBuilder<A> {

    private final DistributedSupplier<A> createFn;

    AggregateOperationBuilder(DistributedSupplier<A> createFn) {
        this.createFn = createFn;
    }

    public <T0> Arity1<T0, A> andAccumulate(DistributedBiConsumer<? super A, T0> accumulateFn) {
        checkNotNull(accumulateFn, "accumulateFn");
        return new Arity1<>(createFn, accumulateFn);
    }

    public <T0> Arity1<T0, A> andAccumulate0(DistributedBiConsumer<? super A, T0> accumulateFn0) {
        checkNotNull(accumulateFn0, "accumulateFn0");
        return new Arity1<>(createFn, accumulateFn0);
    }

    public <T> VarArity<A> andAccumulate(Tag<T> tag, DistributedBiConsumer<? super A, T> accumulateFn) {
        checkNotNull(tag, "tag");
        checkNotNull(accumulateFn, "accumulateFn");
        return new VarArity<>(createFn, tag, accumulateFn);
    }

    public static class Arity1<T0, A> {
        private final DistributedSupplier<A> createFn;
        private final DistributedBiConsumer<? super A, T0> accumulateFn0;
        private DistributedBiConsumer<? super A, ? super A> combineFn;
        private DistributedBiConsumer<? super A, ? super A> deductFn;

        Arity1(DistributedSupplier<A> createFn, DistributedBiConsumer<? super A, T0> accumulateFn0) {
            this.createFn = createFn;
            this.accumulateFn0 = accumulateFn0;
        }

        public <T1> Arity2<T0, T1, A> andAccumulate1(DistributedBiConsumer<? super A, T1> accumulateFn1) {
            checkNotNull(accumulateFn1, "accumulateFn1");
            return new Arity2<>(this, accumulateFn1);
        }

        public Arity1<T0, A> andCombine(DistributedBiConsumer<? super A, ? super A> combineFn) {
            checkNotNull(combineFn, "combineFn");
            this.combineFn = combineFn;
            return this;
        }

        public Arity1<T0, A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductFn) {
            this.deductFn = deductFn;
            return this;
        }

        public <R> AggregateOperation1<T0, A, R> andFinish(DistributedFunction<? super A, R> finishFn) {
            checkNotNull(finishFn, "finishFn");
            return new AggregateOperation1Impl<>(createFn, accumulateFn0, combineFn, deductFn, finishFn);
        }

        public AggregateOperation1<T0, A, A> andIdentityFinish() {
            return new AggregateOperation1Impl<>(createFn, accumulateFn0, combineFn, deductFn,
                    DistributedFunction.identity());
        }
    }

    public static class Arity2<T0, T1, A> {
        private final DistributedSupplier<A> createFn;
        private final DistributedBiConsumer<? super A, T0> accumulateFn0;
        private final DistributedBiConsumer<? super A, T1> accumulateFn1;
        private DistributedBiConsumer<? super A, ? super A> combineFn;
        private DistributedBiConsumer<? super A, ? super A> deductFn;

        Arity2(Arity1<T0, A> step1, DistributedBiConsumer<? super A, T1> accumulateFn1) {
            this.createFn = step1.createFn;
            this.accumulateFn0 = step1.accumulateFn0;
            this.accumulateFn1 = accumulateFn1;
        }

        public <T2> Arity3<T0, T1, T2, A> andAccumulate2(DistributedBiConsumer<? super A, T2> accumulateFn2) {
            checkNotNull(accumulateFn2, "accumulateFn2");
            return new Arity3<>(this, accumulateFn2);
        }

        public Arity2<T0, T1, A> andCombine(DistributedBiConsumer<? super A, ? super A> combineFn) {
            checkNotNull(combineFn, "combineFn");
            this.combineFn = combineFn;
            return this;
        }

        public Arity2<T0, T1, A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductFn) {
            this.deductFn = deductFn;
            return this;
        }

        public <R> AggregateOperation2<T0, T1, A, R> andFinish(DistributedFunction<? super A, R> finishFn) {
            checkNotNull(finishFn, "finishFn");
            return new AggregateOperation2Impl<>(createFn,
                    accumulateFn0, accumulateFn1,
                    combineFn, deductFn, finishFn);
        }

        public AggregateOperation2<T0, T1, A, A> andIdentityFinish() {
            return new AggregateOperation2Impl<>(createFn,
                    accumulateFn0, accumulateFn1,
                    combineFn, deductFn, DistributedFunction.identity());
        }
    }

    public static class Arity3<T0, T1, T2, A> {
        private final DistributedSupplier<A> createFn;
        private final DistributedBiConsumer<? super A, T0> accumulateFn0;
        private final DistributedBiConsumer<? super A, T1> accumulateFn1;
        private final DistributedBiConsumer<? super A, T2> accumulateFn2;
        private DistributedBiConsumer<? super A, ? super A> combineFn;
        private DistributedBiConsumer<? super A, ? super A> deductFn;

        Arity3(Arity2<T0, T1, A> step2,
               DistributedBiConsumer<? super A, T2> accumulateFn2
        ) {
            this.createFn = step2.createFn;
            this.accumulateFn0 = step2.accumulateFn0;
            this.accumulateFn1 = step2.accumulateFn1;
            this.accumulateFn2 = accumulateFn2;
        }

        public Arity3<T0, T1, T2, A> andCombine(DistributedBiConsumer<? super A, ? super A> combineFn) {
            checkNotNull(combineFn, "combineFn");
            this.combineFn = combineFn;
            return this;
        }

        public Arity3<T0, T1, T2, A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductFn) {
            this.deductFn = deductFn;
            return this;
        }

        public <R> AggregateOperation3<T0, T1, T2, A, R> andFinish(
                DistributedFunction<? super A, R> finishFn
        ) {
            checkNotNull(finishFn, "finishFn");
            return new AggregateOperation3Impl<>(createFn,
                    accumulateFn0, accumulateFn1, accumulateFn2,
                    combineFn, deductFn, finishFn);
        }

        public AggregateOperation3<T0, T1, T2, A, A> andIdentityFinish() {
            return new AggregateOperation3Impl<>(createFn,
                    accumulateFn0, accumulateFn1, accumulateFn2,
                    combineFn, deductFn, DistributedFunction.identity());
        }
    }

    public static class VarArity<A> {
        private final DistributedSupplier<A> createFn;
        private final Map<Integer, DistributedBiConsumer<? super A, ?>> accumulateFnsByTag = new HashMap<>();
        private DistributedBiConsumer<? super A, ? super A> combineFn;
        private DistributedBiConsumer<? super A, ? super A> deductFn;

        <T> VarArity(DistributedSupplier<A> createFn, Tag<T> tag, DistributedBiConsumer<? super A, T> accumulateFn) {
            this.createFn = createFn;
            accumulateFnsByTag.put(tag.index(), accumulateFn);
        }

        public <T> VarArity<A> andAccumulate(Tag<T> tag, DistributedBiConsumer<? super A, T> accumulateFn) {
            checkNotNull(tag, "tag");
            checkNotNull(accumulateFn, "accumulateFn");
            accumulateFnsByTag.merge(tag.index(), accumulateFn, (x, y) -> {
                throw new IllegalArgumentException("Tag with index " + tag.index() + " already registered");
            });
            return this;
        }

        public VarArity<A> andCombine(DistributedBiConsumer<? super A, ? super A> combineFn) {
            checkNotNull(combineFn, "combineFn");
            this.combineFn = combineFn;
            return this;
        }

        public VarArity<A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductFn) {
            this.deductFn = deductFn;
            return this;
        }

        public <R> AggregateOperation<A, R> andFinish(DistributedFunction<? super A, R> finishFn) {
            checkNotNull(finishFn, "finishFn");
            return new AggregateOperationImpl<>(createFn, packAccumulateFns(),
                    combineFn, deductFn, finishFn);
        }

        private DistributedBiConsumer<? super A, ?>[] packAccumulateFns() {
            int size = accumulateFnsByTag.size();
            @SuppressWarnings("unchecked")
            DistributedBiConsumer<? super A, ?>[] accFs = new DistributedBiConsumer[size];
            for (int i = 0; i < size; i++) {
                accFs[i] = accumulateFnsByTag.get(i);
                if (accFs[i] == null) {
                    throw new IllegalStateException("Registered tags' indices are "
                            + accumulateFnsByTag.keySet().stream().sorted().collect(toList())
                            + " but should be " + range(0, size).boxed().collect(toList()));
                }
            }
            return accFs;
        }
    }
}
