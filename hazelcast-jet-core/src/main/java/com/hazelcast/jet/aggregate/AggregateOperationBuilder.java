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

    private final DistributedSupplier<A> createAccumulatorF;

    AggregateOperationBuilder(DistributedSupplier<A> createAccumulatorF) {
        this.createAccumulatorF = createAccumulatorF;
    }

    public <T0> Arity1<T0, A> andAccumulate(DistributedBiConsumer<? super A, T0> accumulateItemF) {
        checkNotNull(accumulateItemF, "accumulateItemF");
        return new Arity1<>(createAccumulatorF, accumulateItemF);
    }

    public <T0> Arity1<T0, A> andAccumulate0(DistributedBiConsumer<? super A, T0> accumulateItemF0) {
        checkNotNull(accumulateItemF0, "accumulateItemF0");
        return new Arity1<>(createAccumulatorF, accumulateItemF0);
    }

    public <T> VarArity<A> andAccumulate(Tag<T> tag, DistributedBiConsumer<? super A, T> accumulateItemF) {
        checkNotNull(tag, "tag");
        checkNotNull(accumulateItemF, "accumulateItemF");
        return new VarArity<>(createAccumulatorF, tag, accumulateItemF);
    }

    public static class Arity1<T0, A> {
        private final DistributedSupplier<A> createAccumulatorF;
        private final DistributedBiConsumer<? super A, T0> accumulateItemF0;
        private DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;
        private DistributedBiConsumer<? super A, ? super A> deductAccumulatorF;

        Arity1(DistributedSupplier<A> createAccumulatorF, DistributedBiConsumer<? super A, T0> accumulateItemF0) {
            this.createAccumulatorF = createAccumulatorF;
            this.accumulateItemF0 = accumulateItemF0;
        }

        public <T1> Arity2<T0, T1, A> andAccumulate1(DistributedBiConsumer<? super A, T1> accumulateItemF1) {
            checkNotNull(accumulateItemF1, "accumulateItemF1");
            return new Arity2<>(this, accumulateItemF1);
        }

        public Arity1<T0, A> andCombine(DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF) {
            checkNotNull(combineAccumulatorsF, "combineAccumulatorsF");
            this.combineAccumulatorsF = combineAccumulatorsF;
            return this;
        }

        public Arity1<T0, A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductAccumulatorF) {
            this.deductAccumulatorF = deductAccumulatorF;
            return this;
        }

        public <R> AggregateOperation1<T0, A, R> andFinish(DistributedFunction<? super A, R> finishAccumulationF) {
            checkNotNull(finishAccumulationF, "finishAccumulationF");
            return new AggregateOperation1Impl<>(createAccumulatorF, accumulateItemF0,
                    combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
        }
    }

    public static class Arity2<T0, T1, A> {
        private final DistributedSupplier<A> createAccumulatorF;
        private final DistributedBiConsumer<? super A, T0> accumulateItemF0;
        private final DistributedBiConsumer<? super A, T1> accumulateItemF1;
        private DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;
        private DistributedBiConsumer<? super A, ? super A> deductAccumulatorF;

        Arity2(Arity1<T0, A> step1, DistributedBiConsumer<? super A, T1> accumulateItemF1) {
            this.createAccumulatorF = step1.createAccumulatorF;
            this.accumulateItemF0 = step1.accumulateItemF0;
            this.accumulateItemF1 = accumulateItemF1;
        }

        public <T2> Arity3<T0, T1, T2, A> andAccumulate2(DistributedBiConsumer<? super A, T2> accumulateItemF2) {
            checkNotNull(accumulateItemF2, "accumulateItemF2");
            return new Arity3<>(this, accumulateItemF2);
        }

        public Arity2<T0, T1, A> andCombine(DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF) {
            checkNotNull(combineAccumulatorsF, "combineAccumulatorsF");
            this.combineAccumulatorsF = combineAccumulatorsF;
            return this;
        }

        public Arity2<T0, T1, A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductAccumulatorF) {
            this.deductAccumulatorF = deductAccumulatorF;
            return this;
        }

        public <R> AggregateOperation2<T0, T1, A, R> andFinish(DistributedFunction<? super A, R> finishAccumulationF) {
            checkNotNull(finishAccumulationF, "finishAccumulationF");
            return new AggregateOperation2Impl<>(createAccumulatorF,
                    accumulateItemF0, accumulateItemF1,
                    combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
        }
    }

    public static class Arity3<T0, T1, T2, A> {
        private final DistributedSupplier<A> createAccumulatorF;
        private final DistributedBiConsumer<? super A, T0> accumulateItemF0;
        private final DistributedBiConsumer<? super A, T1> accumulateItemF1;
        private final DistributedBiConsumer<? super A, T2> accumulateItemF2;
        private DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;
        private DistributedBiConsumer<? super A, ? super A> deductAccumulatorF;

        Arity3(Arity2<T0, T1, A> step2,
               DistributedBiConsumer<? super A, T2> accumulateItemF2
        ) {
            this.createAccumulatorF = step2.createAccumulatorF;
            this.accumulateItemF0 = step2.accumulateItemF0;
            this.accumulateItemF1 = step2.accumulateItemF1;
            this.accumulateItemF2 = accumulateItemF2;
        }

        public Arity3<T0, T1, T2, A> andCombine(DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF) {
            checkNotNull(combineAccumulatorsF, "combineAccumulatorsF");
            this.combineAccumulatorsF = combineAccumulatorsF;
            return this;
        }

        public Arity3<T0, T1, T2, A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductAccumulatorF) {
            this.deductAccumulatorF = deductAccumulatorF;
            return this;
        }

        public <R> AggregateOperation3<T0, T1, T2, A, R> andFinish(
                DistributedFunction<? super A, R> finishAccumulationF
        ) {
            checkNotNull(finishAccumulationF, "finishAccumulationF");
            return new AggregateOperation3Impl<>(createAccumulatorF,
                    accumulateItemF0, accumulateItemF1, accumulateItemF2,
                    combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
        }
    }

    public static class VarArity<A> {
        private final DistributedSupplier<A> createAccumulatorF;
        private final Map<Integer, DistributedBiConsumer<? super A, ?>> accumulateFsByTag = new HashMap<>();
        private DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;
        private DistributedBiConsumer<? super A, ? super A> deductAccumulatorF;

        <T> VarArity(
                DistributedSupplier<A> createAccumulatorF,
                Tag<T> tag,
                DistributedBiConsumer<? super A, T> accumulateItemF
        ) {
            this.createAccumulatorF = createAccumulatorF;
            accumulateFsByTag.put(tag.index(), accumulateItemF);
        }

        public <T> VarArity<A> andAccumulate(Tag<T> tag, DistributedBiConsumer<? super A, T> accumulateItemF) {
            checkNotNull(tag, "tag");
            checkNotNull(accumulateItemF, "accumulateItemF");
            accumulateFsByTag.merge(tag.index(), accumulateItemF, (x, y) -> {
                throw new IllegalArgumentException("Tag with index " + tag.index() + " already registered");
            });
            return this;
        }

        public VarArity<A> andCombine(DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF) {
            checkNotNull(combineAccumulatorsF, "combineAccumulatorsF");
            this.combineAccumulatorsF = combineAccumulatorsF;
            return this;
        }

        public VarArity<A> andDeduct(DistributedBiConsumer<? super A, ? super A> deductAccumulatorF) {
            checkNotNull(deductAccumulatorF, "deductAccumulatorF");
            this.deductAccumulatorF = deductAccumulatorF;
            return this;
        }

        public <R> AggregateOperation<A, R> andFinish(DistributedFunction<? super A, R> finishAccumulationF) {
            checkNotNull(finishAccumulationF, "finishAccumulationF");
            return new AggregateOperationImpl<>(createAccumulatorF, packAccumulateFs(),
                    combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
        }

        private DistributedBiConsumer<? super A, ?>[] packAccumulateFs() {
            int size = accumulateFsByTag.size();
            @SuppressWarnings("unchecked")
            DistributedBiConsumer<? super A, ?>[] accFs = new DistributedBiConsumer[size];
            for (int i = 0; i < size; i++) {
                accFs[i] = accumulateFsByTag.get(i);
                if (accFs[i] == null) {
                    throw new IllegalStateException("Registered tags' indices are "
                            + accumulateFsByTag.keySet().stream().sorted().collect(toList())
                            + " but should be " + range(0, size).boxed().collect(toList()));
                }
            }
            return accFs;
        }
    }
}
