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

package com.hazelcast.jet.processor;

import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.connector.WriteLoggerP;
import com.hazelcast.jet.impl.processor.PeekWrappedP;
import com.hazelcast.jet.impl.util.WrappingProcessorMetaSupplier;
import com.hazelcast.jet.impl.util.WrappingProcessorSupplier;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.function.DistributedFunctions.alwaysTrue;

/**
 * Static utility class with factories of sinks and wrappers that log
 * the data flowing through the DAG. These processors are useful while
 * diagnosing the execution of Jet jobs. For other kinds of processors
 * refer to the {@link com.hazelcast.jet.processor package-level
 * documentation}.
 */
public final class DiagnosticProcessors {
    private DiagnosticProcessors() {
    }

    /**
     * Returns a supplier of processor that acts as a sink and logs all items
     * at the INFO level. {@link Watermark} items are not logged.
     * <p>
     * Note that the event will be logged on the cluster members, not on the
     * client, so it's mainly useful as a diagnostic tool.
     * <p>
     * {@link Vertex#localParallelism(int) Local parallelism} of 1 is
     * recommended for this vertex.
     *
     * @param toStringF Function to convert item to String.
     * @param <T> input item type
     */
    @Nonnull
    public static <T> DistributedSupplier<Processor> writeLogger(
            @Nonnull DistributedFunction<T, String> toStringF
    ) {
        return () -> new WriteLoggerP(toStringF);
    }

    /**
     * Convenience for {@link #writeLogger(DistributedFunction)} that uses
     * {@code toString()} as {@code toStringF}.
     */
    @Nonnull
    public static DistributedSupplier<Processor> writeLogger() {
        return writeLogger(Object::toString);
    }

    /**
     * Returns a meta-supplier that will add logging to the processors created
     * by the provided meta-supplier. Each item the processor removes from the
     * inbox will be logged. Items are logged at the INFO level to the
     * following logging category: {@link PeekWrappedP}.
     * <p>
     * <strong>Warning:</strong> The {@code toStringF} and {@code shouldLogF}
     * functions will see all items, including {@link Watermark}s.
     *
     * @param toStringF function that returns the string representation of the item
     * @param shouldLogF function to filter logged items. {@link
     *                   com.hazelcast.jet.function.DistributedFunctions#alwaysTrue()} can be
     *                   used as a pass-through filter when no filtering is needed.
     * @param wrapped The wrapped meta-supplier.
     *
     * @see #peekOutput(DistributedFunction, DistributedPredicate, ProcessorMetaSupplier)
     */
    @Nonnull
    public static ProcessorMetaSupplier peekInput(
            @Nonnull DistributedFunction<Object, String> toStringF,
            @Nonnull DistributedPredicate<Object> shouldLogF,
            @Nonnull ProcessorMetaSupplier wrapped
    ) {
        return new WrappingProcessorMetaSupplier(wrapped, p -> new PeekWrappedP(p, toStringF, shouldLogF, true, false));
    }

    /**
     * Same as {@link #peekInput(DistributedFunction, DistributedPredicate,
     * ProcessorMetaSupplier) peekInput(toStringF, shouldLogF, metaSupplier)},
     * but accepts a {@code ProcessorSupplier} instead of a meta-supplier.
     */
    @Nonnull
    public static ProcessorSupplier peekInput(
            @Nonnull DistributedFunction<Object, String> toStringF,
            @Nonnull DistributedPredicate<Object> shouldLogF,
            @Nonnull ProcessorSupplier wrapped
    ) {
        return new WrappingProcessorSupplier(wrapped, p -> new PeekWrappedP(p, toStringF, shouldLogF, true, false));
    }

    /**
     * Same as {@link #peekInput(DistributedFunction, DistributedPredicate,
     * ProcessorMetaSupplier) peekInput(toStringF, shouldLogF, metaSupplier)},
     * but accepts a {@code DistributedSupplier} of processors instead of a
     * meta-supplier.
     */
    @Nonnull
    public static DistributedSupplier<Processor> peekInput(
            @Nonnull DistributedFunction<Object, String> toStringF,
            @Nonnull DistributedPredicate<Object> shouldLogF,
            @Nonnull DistributedSupplier<Processor> wrapped
    ) {
        return () -> new PeekWrappedP(wrapped.get(), toStringF, shouldLogF, true, false);
    }

    /**
     * Convenience for {@link #peekInput(DistributedFunction,
     * DistributedPredicate, ProcessorMetaSupplier) peekInput(toStringF,
     * shouldLogF, metaSupplier)} with a pass-through filter and {@code
     * Object#toString} as the formatting function.
     */
    @Nonnull
    public static ProcessorMetaSupplier peekInput(@Nonnull ProcessorMetaSupplier wrapped) {
        return peekInput(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Convenience for {@link #peekInput(DistributedFunction,
     * DistributedPredicate, ProcessorMetaSupplier) peekInput(toStringF,
     * shouldLogF, metaSupplier)} with a pass-through filter and {@code
     * Object#toString} as the formatting function. This variant accepts a
     * {@code ProcessorSupplier} instead of a meta-supplier.
     */
    @Nonnull
    public static ProcessorSupplier peekInput(@Nonnull ProcessorSupplier wrapped) {
        return peekInput(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Convenience for {@link #peekInput(DistributedFunction,
     * DistributedPredicate, ProcessorMetaSupplier) peekInput(toStringF,
     * shouldLogF, metaSupplier)} with a pass-through filter and {@code
     * Object#toString} as the formatting function. This variant accepts a
     * {@code DistributedSupplier} of processors instead of a meta-supplier.
     */
    @Nonnull
    public static DistributedSupplier<Processor> peekInput(@Nonnull DistributedSupplier<Processor> wrapped) {
        return peekInput(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Returns a meta-supplier that will add logging to the processors created
     * by the provided meta-supplier. Each item the processor adds to the
     * outbox will be logged. Items are logged at the INFO level to the
     * following logging category: {@link PeekWrappedP}.
     * <p>
     * <strong>Warning:</strong> The {@code toStringF} and {@code shouldLogF}
     * functions will see all items, including {@link Watermark}s.
     *
     * @param toStringF function that returns the string representation of the item
     * @param shouldLogF function to filter logged items. {@link
     *                   com.hazelcast.jet.function.DistributedFunctions#alwaysTrue()} can be
     *                   used as a pass-through filter when no filtering is needed.
     * @param wrapped The wrapped meta-supplier.
     *
     * @see #peekInput(DistributedFunction, DistributedPredicate, ProcessorMetaSupplier)
     */
    @Nonnull
    public static ProcessorMetaSupplier peekOutput(
            @Nonnull DistributedFunction<Object, String> toStringF,
            @Nonnull DistributedPredicate<Object> shouldLogF,
            @Nonnull ProcessorMetaSupplier wrapped
    ) {
        return new WrappingProcessorMetaSupplier(wrapped, p -> new PeekWrappedP(p, toStringF, shouldLogF, false, true));
    }

    /**
     * Same as {@link #peekOutput(DistributedFunction, DistributedPredicate,
     * ProcessorMetaSupplier) peekOutput(toStringF, shouldLogF, metaSupplier)},
     * but accepts a {@code ProcessorSupplier} instead of a meta-supplier.
     */
    @Nonnull
    public static ProcessorSupplier peekOutput(
            @Nonnull DistributedFunction<Object, String> toStringF,
            @Nonnull DistributedPredicate<Object> shouldLogF,
            @Nonnull ProcessorSupplier wrapped
    ) {
        return new WrappingProcessorSupplier(wrapped, p -> new PeekWrappedP(p, toStringF, shouldLogF, false, true));
    }

    /**
     * Same as {@link #peekOutput(DistributedFunction, DistributedPredicate,
     * ProcessorMetaSupplier) peekOutput(toStringF, shouldLogF, metaSupplier)},
     * but accepts a {@code DistributedSupplier} of processors instead of a
     * meta-supplier.
     */
    @Nonnull
    public static DistributedSupplier<Processor> peekOutput(
            @Nonnull DistributedFunction<Object, String> toStringF,
            @Nonnull DistributedPredicate<Object> shouldLogF,
            @Nonnull DistributedSupplier<Processor> wrapped) {
        return () -> new PeekWrappedP(wrapped.get(), toStringF, shouldLogF, false, true);
    }

    /**
     * Convenience for {@link #peekOutput(DistributedFunction,
     * DistributedPredicate, ProcessorMetaSupplier) peekOutput(toStringF,
     * shouldLogF, metaSupplier} with a pass-through filter and {@code
     * Object#toString} as the formatting function.
     */
    @Nonnull
    public static ProcessorMetaSupplier peekOutput(@Nonnull ProcessorMetaSupplier wrapped) {
        return peekOutput(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Convenience for {@link #peekOutput(DistributedFunction,
     * DistributedPredicate, ProcessorMetaSupplier) peekOutput(toStringF,
     * shouldLogF, metaSupplier} with a pass-through filter and {@code
     * Object#toString} as the formatting function. This variant accepts a
     * {@code ProcessorSupplier} instead of a meta-supplier.
     */
    @Nonnull
    public static ProcessorSupplier peekOutput(@Nonnull ProcessorSupplier wrapped) {
        return peekOutput(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Convenience for {@link #peekOutput(DistributedFunction,
     * DistributedPredicate, ProcessorMetaSupplier) peekOutput(toStringF,
     * shouldLogF, metaSupplier} with a pass-through filter and {@code
     * Object#toString} as the formatting function. This variant accepts a
     * {@code DistributedSupplier} of processors instead of a meta-supplier.
     */
    @Nonnull
    public static DistributedSupplier<Processor> peekOutput(@Nonnull DistributedSupplier<Processor> wrapped) {
        return peekOutput(Object::toString, alwaysTrue(), wrapped);
    }
}
