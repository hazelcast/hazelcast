/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.aggregate.AggregateOperation1Impl;
import com.hazelcast.jet.impl.aggregate.AggregateOperation2Impl;
import com.hazelcast.jet.impl.aggregate.AggregateOperation3Impl;
import com.hazelcast.jet.impl.aggregate.AggregateOperationImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

/**
 * A builder object that can be used to construct the definition of an
 * aggregate operation in a step-by-step manner. Please refer to
 * {@link AggregateOperation#withCreate(DistributedSupplier)
 * AggregateOperation.withCreate()} for more details.
 *
 * @param <A> the type of the accumulator
 */
public final class AggregateOperationBuilder<A> {

    @Nonnull
    private final DistributedSupplier<A> createFn;

    AggregateOperationBuilder(@Nonnull DistributedSupplier<A> createFn) {
        this.createFn = createFn;
    }

    /**
     * Registers the supplied {@code accumulate} primitive. Also selects the
     * fixed-arity variant of the aggregate operation.
     * <p>
     * This method is synonymous with {@link #andAccumulate0(
     * DistributedBiConsumer)}, but makes more sense when defining a
     * simple, arity-1 aggregate operation.
     *
     * @param accumulateFn the {@code accumulate} primitive, parameters are
     *              {@code (accumulator, item)}
     * @param <T> the expected type of input item
     * @return a new builder object that captures the {@code T0} type parameter
     */
    @Nonnull
    public <T> Arity1<T, A, Void> andAccumulate(@Nonnull DistributedBiConsumer<? super A, ? super T> accumulateFn) {
        checkSerializable(accumulateFn, "accumulateFn");
        return new Arity1<>(createFn, accumulateFn);
    }

    /**
     * Registers the supplied {@code accumulate} primitive for stream-0.
     * Also selects the fixed-arity variant of the aggregate operation.
     *
     * @param accumulateFn0 the {@code accumulate} primitive for stream-0
     * @param <T0> the expected type of item in stream-0
     * @return a new builder object that captures the {@code T0} type parameter
     */
    @Nonnull
    public <T0> Arity1<T0, A, Void> andAccumulate0(@Nonnull DistributedBiConsumer<? super A, ? super T0> accumulateFn0) {
        checkSerializable(accumulateFn0, "accumulateFn0");
        return new Arity1<>(createFn, accumulateFn0);
    }

    /**
     * Selects the variable-arity variant for this aggregate operation builder.
     *
     * @return a new builder object for variable-arity aggregate operations which has
     *         the {@code createFn} of the current builder
     */
    public VarArity<A, Void> varArity() {
        return new VarArity<>(createFn);
    }

    /**
     * Registers the supplied {@code accumulate} primitive for the stream tagged
     * with the supplied tag. Also selects the variable-arity variant of the
     * aggregate operation.
     *
     * @param tag the tag of the associated input stream
     * @param accumulateFn the {@code accumulate} primitive
     * @param <T> the expected type of input item
     * @return a new builder object for variable-arity aggregate operations which has
     *         the {@code createFn} of the current builder
     */
    @Nonnull
    public <T> VarArity<A, Void> andAccumulate(
            @Nonnull Tag<T> tag, @Nonnull DistributedBiConsumer<? super A, ? super T> accumulateFn
    ) {
        checkSerializable(accumulateFn, "accumulateFn");
        return new VarArity<>(createFn, tag, accumulateFn);
    }

    /**
     * The arity-1 variant of the aggregate operation builder. Can be
     * raised to arity-2 by calling {@link #andAccumulate1(
     * DistributedBiConsumer) andAccumulate1()}.
     *
     * @param <T0> type of item in stream-0
     * @param <A> type of the accumulator
     * @param <R> type of the aggregation result
     */
    public static class Arity1<T0, A, R> {
        @Nonnull
        private final DistributedSupplier<A> createFn;
        @Nonnull
        private final DistributedBiConsumer<? super A, ? super T0> accumulateFn0;
        private DistributedBiConsumer<? super A, ? super A> combineFn;
        private DistributedBiConsumer<? super A, ? super A> deductFn;
        private DistributedFunction<? super A, ? extends R> exportFn;

        Arity1(
                @Nonnull DistributedSupplier<A> createFn,
                @Nonnull DistributedBiConsumer<? super A, ? super T0> accumulateFn0
        ) {
            this.createFn = createFn;
            this.accumulateFn0 = accumulateFn0;
        }

        /**
         * Registers the supplied {@code accumulate} primitive for stream-1,
         * returning the arity-2 variant of the builder.
         *
         * @param accumulateFn1 the {@code accumulate} primitive for stream-1
         * @param <T1> the expected type of item in stream-1
         * @return a new builder object that captures the {@code T1} type parameter
         */
        @Nonnull
        public <T1> Arity2<T0, T1, A, R> andAccumulate1(
                @Nonnull DistributedBiConsumer<? super A, ? super T1> accumulateFn1
        ) {
            checkSerializable(accumulateFn1, "accumulateFn1");
            return new Arity2<>(this, accumulateFn1);
        }

        /**
         * Registers the {@code combine} primitive.
         */
        @Nonnull
        public Arity1<T0, A, R> andCombine(@Nullable DistributedBiConsumer<? super A, ? super A> combineFn) {
            checkSerializable(combineFn, "combineFn");
            this.combineFn = combineFn;
            return this;
        }

        /**
         * Registers the {@code deduct} primitive.
         */
        @Nonnull
        public Arity1<T0, A, R> andDeduct(@Nullable DistributedBiConsumer<? super A, ? super A> deductFn) {
            checkSerializable(deductFn, "deductFn");
            this.deductFn = deductFn;
            return this;
        }

        /**
         * Registers the {@code export} primitive.
         */
        @Nonnull
        public <R_NEW> Arity1<T0, A, R_NEW> andExport(
                @Nonnull DistributedFunction<? super A, ? extends R_NEW> exportFn
        ) {
            checkSerializable(exportFn, "exportFn");
            @SuppressWarnings("unchecked")
            Arity1<T0, A, R_NEW> newThis = (Arity1<T0, A, R_NEW>) this;
            newThis.exportFn = exportFn;
            return newThis;
        }

        /**
         * Registers the supplied function as the {@code finish} primitive. Constructs
         * and returns an {@link AggregateOperation1} from the current state of the
         * builder.
         *
         * @throws IllegalStateException if the {@code export} primitive was
         * not registered
         */
        @Nonnull
        public AggregateOperation1<T0, A, R> andFinish(
                @Nonnull DistributedFunction<? super A, ? extends R> finishFn
        ) {
            if (exportFn == null) {
                throw new IllegalStateException(
                        "The export primitive is not registered. Either add the missing andExport() call" +
                        " or use andExportFinish() to register the same function as both the export and" +
                        " finish primitive");
            }
            checkSerializable(finishFn, "finishFn");
            return new AggregateOperation1Impl<>(
                    createFn, accumulateFn0, combineFn, deductFn, exportFn, finishFn);
        }

        /**
         * Registers the supplied function as both the {@code export} and {@code
         * finish} primitive. Constructs and returns an {@link AggregateOperation1}
         * from the current state of the builder.
         *
         * @throws IllegalStateException if the {@code export} primitive is
         * already registered
         */
        @Nonnull
        public <R_NEW> AggregateOperation1<T0, A, R_NEW> andExportFinish(
                @Nonnull DistributedFunction<? super A, ? extends R_NEW> exportFinishFn
        ) {
            if (exportFn != null) {
                throw new IllegalStateException("The export primitive is already registered. Call" +
                        " andFinish() if you want to register a separate finish primitive.");
            }
            checkSerializable(exportFinishFn, "exportFinishFn");
            return new AggregateOperation1Impl<>(
                    createFn, accumulateFn0, combineFn, deductFn, exportFinishFn, exportFinishFn);
        }
    }

    /**
     * The arity-2 variant of the aggregate operation builder. Can be
     * raised to arity-3 by calling {@link #andAccumulate2(
     * DistributedBiConsumer) andAccumulate2()}.
     *
     * @param <T0> the type of item in stream-0
     * @param <T1> the type of item in stream-1
     * @param <A> the type of the accumulator
     * @param <R> type of the aggregation result
     */
    public static class Arity2<T0, T1, A, R> {
        @Nonnull
        private final DistributedSupplier<A> createFn;
        @Nonnull
        private final DistributedBiConsumer<? super A, ? super T0> accumulateFn0;
        @Nonnull
        private final DistributedBiConsumer<? super A, ? super T1> accumulateFn1;
        private DistributedBiConsumer<? super A, ? super A> combineFn;
        private DistributedBiConsumer<? super A, ? super A> deductFn;
        private DistributedFunction<? super A, ? extends R> exportFn;

        Arity2(@Nonnull Arity1<T0, A, R> step1, @Nonnull DistributedBiConsumer<? super A, ? super T1> accumulateFn1) {
            this.createFn = step1.createFn;
            this.accumulateFn0 = step1.accumulateFn0;
            this.accumulateFn1 = accumulateFn1;
        }

        /**
         * Registers the supplied {@code accumulate} primitive for stream-2,
         * returning the arity-3 variant of the builder.
         *
         * @param accumulateFn2 the {@code accumulate} primitive for stream-2
         * @param <T2> the expected type of item in stream-2
         * @return a new builder object that captures the {@code T2} type parameter
         */
        @Nonnull
        public <T2> Arity3<T0, T1, T2, A, R> andAccumulate2(
                @Nonnull DistributedBiConsumer<? super A, ? super T2> accumulateFn2
        ) {
            checkSerializable(accumulateFn2, "accumulateFn2");
            return new Arity3<>(this, accumulateFn2);
        }

        /**
         * Registers the {@code combine} primitive.
         */
        @Nonnull
        public Arity2<T0, T1, A, R> andCombine(@Nullable DistributedBiConsumer<? super A, ? super A> combineFn) {
            checkSerializable(combineFn, "combineFn");
            this.combineFn = combineFn;
            return this;
        }

        /**
         * Registers the {@code deduct} primitive.
         */
        @Nonnull
        public Arity2<T0, T1, A, R> andDeduct(@Nullable DistributedBiConsumer<? super A, ? super A> deductFn) {
            checkSerializable(deductFn, "deductFn");
            this.deductFn = deductFn;
            return this;
        }

        /**
         * Registers the {@code export} primitive.
         */
        @Nonnull
        public <R_NEW> Arity2<T0, T1, A, R_NEW> andExport(
                @Nonnull DistributedFunction<? super A, ? extends R_NEW> exportFn
        ) {
            checkSerializable(exportFn, "exportFn");
            @SuppressWarnings("unchecked")
            Arity2<T0, T1, A, R_NEW> newThis = (Arity2<T0, T1, A, R_NEW>) this;
            newThis.exportFn = exportFn;
            return newThis;
        }

        /**
         * Registers the supplied function as the {@code finish} primitive. Constructs
         * and returns an {@link AggregateOperation1} from the current state of the
         * builder.
         *
         * @throws IllegalStateException if the {@code export} primitive was
         * not registered
         */
        @Nonnull
        public AggregateOperation2<T0, T1, A, R> andFinish(
                @Nonnull DistributedFunction<? super A, ? extends R> finishFn
        ) {
            checkSerializable(finishFn, "finishFn");
            if (exportFn == null) {
                throw new IllegalStateException(
                        "The export primitive is not registered. Either add the missing andExport() call" +
                        " or use andExportFinish() to register the same function as both the export and" +
                        " finish primitive");
            }
            return new AggregateOperation2Impl<>(
                    createFn, accumulateFn0, accumulateFn1, combineFn, deductFn, exportFn, finishFn);
        }

        /**
         * Registers the supplied function as both the {@code export} and {@code
         * finish} primitive. Constructs and returns an {@link AggregateOperation2}
         * from the current state of the builder.
         *
         * @throws IllegalStateException if the {@code export} primitive is
         * already registered
         */
        @Nonnull
        public <R_NEW> AggregateOperation2<T0, T1, A, R_NEW> andExportFinish(
                @Nonnull DistributedFunction<? super A, ? extends R_NEW> exportFinishFn
        ) {
            return new AggregateOperation2Impl<>(createFn, accumulateFn0, accumulateFn1,
                    combineFn, deductFn, exportFinishFn, exportFinishFn);
        }
    }

    /**
     * The arity-3 variant of the aggregate operation builder.
     *
     * @param <T0> the type of item in stream-0
     * @param <T1> the type of item in stream-1
     * @param <T2> the type of item in stream-2
     * @param <A> the type of the accumulator
     * @param <R> type of the aggregation result
     */
    public static class Arity3<T0, T1, T2, A, R> {
        @Nonnull
        private final DistributedSupplier<A> createFn;
        @Nonnull
        private final DistributedBiConsumer<? super A, ? super T0> accumulateFn0;
        @Nonnull
        private final DistributedBiConsumer<? super A, ? super T1> accumulateFn1;
        @Nonnull
        private final DistributedBiConsumer<? super A, ? super T2> accumulateFn2;
        private DistributedBiConsumer<? super A, ? super A> combineFn;
        private DistributedBiConsumer<? super A, ? super A> deductFn;
        private DistributedFunction<? super A, ? extends R> exportFn;

        Arity3(Arity2<T0, T1, A, R> arity2, DistributedBiConsumer<? super A, ? super T2> accumulateFn2) {
            this.createFn = arity2.createFn;
            this.accumulateFn0 = arity2.accumulateFn0;
            this.accumulateFn1 = arity2.accumulateFn1;
            this.accumulateFn2 = accumulateFn2;
        }

        /**
         * Registers the {@code combine} primitive.
         */
        @Nonnull
        public Arity3<T0, T1, T2, A, R> andCombine(@Nullable DistributedBiConsumer<? super A, ? super A> combineFn) {
            checkSerializable(combineFn, "combineFn");
            this.combineFn = combineFn;
            return this;
        }

        /**
         * Registers the {@code deduct} primitive.
         */
        @Nonnull
        public Arity3<T0, T1, T2, A, R> andDeduct(@Nullable DistributedBiConsumer<? super A, ? super A> deductFn) {
            checkSerializable(deductFn, "deductFn");
            this.deductFn = deductFn;
            return this;
        }

        /**
         * Registers the {@code export} primitive.
         */
        @Nonnull
        public <R_NEW> Arity3<T0, T1, T2, A, R_NEW> andExport(
                @Nonnull DistributedFunction<? super A, ? extends R_NEW> exportFn
        ) {
            checkSerializable(exportFn, "exportFn");
            @SuppressWarnings("unchecked")
            Arity3<T0, T1, T2, A, R_NEW> newThis = (Arity3<T0, T1, T2, A, R_NEW>) this;
            newThis.exportFn = exportFn;
            return newThis;
        }

        /**
         * Registers the supplied function as the {@code finish} primitive. Constructs
         * and returns an {@link AggregateOperation1} from the current state of the
         * builder.
         *
         * @throws IllegalStateException if the {@code export} primitive was
         * not registered
         */
        @Nonnull
        public AggregateOperation3<T0, T1, T2, A, R> andFinish(
                @Nonnull DistributedFunction<? super A, ? extends R> finishFn
        ) {
            if (exportFn == null) {
                throw new IllegalStateException(
                        "The export primitive is not registered. Either add the missing andExport() call" +
                                " or use andExportFinish() to register the same function as both the export and" +
                                " finish primitive");
            }
            checkSerializable(finishFn, "finishFn");
            return new AggregateOperation3Impl<>(createFn,
                    accumulateFn0, accumulateFn1, accumulateFn2,
                    combineFn, deductFn, exportFn, finishFn);
        }

        /**
         * Registers the supplied function as both the {@code export} and {@code
         * finish} primitive. Constructs and returns an {@link AggregateOperation3}
         * from the current state of the builder.
         *
         * @throws IllegalStateException if the {@code export} primitive is
         * already registered
         */
        @Nonnull
        public <R_NEW> AggregateOperation3<T0, T1, T2, A, R_NEW> andExportFinish(
                @Nonnull DistributedFunction<? super A, ? extends R_NEW> exportFinishFn
        ) {
            checkSerializable(exportFinishFn, "exportFinishFn");
            return new AggregateOperation3Impl<>(createFn, accumulateFn0, accumulateFn1, accumulateFn2,
                    combineFn, deductFn, exportFinishFn, exportFinishFn);
        }
    }

    /**
     * The variable-arity variant of the aggregate operation builder.
     * Accepts any number of {@code accumulate} primitives and associates
     * them with {@link Tag}s. Does not capture the static type of the
     * stream items.
     *
     * @param <A> the type of the accumulator
     * @param <R> type of the aggregation result
     */
    public static class VarArity<A, R> {
        @Nonnull
        private final DistributedSupplier<A> createFn;
        private final Map<Integer, DistributedBiConsumer<? super A, ?>> accumulateFnsByTag = new HashMap<>();
        private DistributedBiConsumer<? super A, ? super A> combineFn;
        private DistributedBiConsumer<? super A, ? super A> deductFn;
        private DistributedFunction<? super A, ? extends R> exportFn;

        VarArity(@Nonnull DistributedSupplier<A> createFn) {
            this.createFn = createFn;
        }

        <T> VarArity(
                @Nonnull DistributedSupplier<A> createFn,
                @Nonnull Tag<T> tag,
                @Nonnull DistributedBiConsumer<? super A, ? super T> accumulateFn
        ) {
            this(createFn);
            accumulateFnsByTag.put(tag.index(), accumulateFn);
        }

        /**
         * Registers the supplied {@code accumulate} primitive for the stream tagged
         * with the supplied tag.
         *
         * @param tag the tag of the associated input stream
         * @param accumulateFn the {@code accumulate} primitive
         * @param <T> the expected type of input item
         * @return this
         */
        @Nonnull
        public <T> VarArity<A, R> andAccumulate(
                @Nonnull Tag<T> tag, @Nonnull DistributedBiConsumer<? super A, T> accumulateFn
        ) {
            checkSerializable(accumulateFn, "accumulateFn");
            if (accumulateFnsByTag.putIfAbsent(tag.index(), accumulateFn) != null) {
                throw new IllegalArgumentException("Tag with index " + tag.index() + " already registered");
            }
            return this;
        }

        /**
         * Registers the {@code combine} primitive.
         */
        @Nonnull
        public VarArity<A, R> andCombine(@Nullable DistributedBiConsumer<? super A, ? super A> combineFn) {
            checkSerializable(combineFn, "combineFn");
            this.combineFn = combineFn;
            return this;
        }

        /**
         * Registers the {@code deduct} primitive.
         */
        @Nonnull
        public VarArity<A, R> andDeduct(@Nullable DistributedBiConsumer<? super A, ? super A> deductFn) {
            checkSerializable(deductFn, "deductFn");
            this.deductFn = deductFn;
            return this;
        }

        /**
         * Registers the {@code export} primitive.
         */
        @Nonnull
        public <R_NEW> VarArity<A, R_NEW> andExport(
                @Nonnull DistributedFunction<? super A, ? extends R_NEW> exportFn
        ) {
            checkSerializable(exportFn, "exportFn");
            @SuppressWarnings("unchecked")
            VarArity<A, R_NEW> newThis = (VarArity<A, R_NEW>) this;
            newThis.exportFn = exportFn;
            return newThis;
        }

        /**
         * Registers the supplied function as the {@code finish} primitive. Constructs
         * and returns an {@link AggregateOperation1} from the current state of the
         * builder.
         *
         * @throws IllegalStateException if the {@code export} primitive was
         * not registered
         */
        @Nonnull
        public AggregateOperation<A, R> andFinish(
                @Nonnull DistributedFunction<? super A, ? extends R> finishFn
        ) {
            if (exportFn == null) {
                throw new IllegalStateException(
                        "The export primitive is not registered. Either add the missing andExport() call" +
                                " or use andExportFinish() to register the same function as both the export and" +
                                " finish primitive");
            }
            checkSerializable(finishFn, "finishFn");
            return new AggregateOperationImpl<>(
                    createFn, packAccumulateFns(), combineFn, deductFn, exportFn, finishFn);
        }

        /**
         * Registers the supplied function as both the {@code export} and {@code
         * finish} primitive. Constructs and returns an {@link AggregateOperation1}
         * from the current state of the builder.
         *
         * @throws IllegalStateException if the {@code export} primitive is
         * already registered
         */
        @Nonnull
        public <R_NEW> AggregateOperation<A, R_NEW> andExportFinish(
                @Nonnull DistributedFunction<? super A, ? extends R_NEW> exportFinishFn
        ) {
            if (exportFn != null) {
                throw new IllegalStateException("The export primitive is already registered. Call" +
                        " andFinish() if you want to register a separate finish primitive.");
            }
            checkSerializable(exportFinishFn, "exportFinishFn");
            return new AggregateOperationImpl<>(
                    createFn, packAccumulateFns(), combineFn, deductFn, exportFinishFn, exportFinishFn);
        }

        private DistributedBiConsumer<? super A, ?>[] packAccumulateFns() {
            int size = accumulateFnsByTag.size();
            @SuppressWarnings("unchecked")
            DistributedBiConsumer<? super A, ?>[] accFns = new DistributedBiConsumer[size];
            for (int i = 0; i < size; i++) {
                accFns[i] = accumulateFnsByTag.get(i);
                if (accFns[i] == null) {
                    throw new IllegalStateException("Registered tags' indices are "
                            + accumulateFnsByTag.keySet().stream().sorted().collect(toList())
                            + " but should be " + range(0, size).boxed().collect(toList()));
                }
            }
            return accFns;
        }
    }
}
