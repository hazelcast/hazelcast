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

package com.hazelcast.jet.core.processor;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.impl.connector.WriteLoggerP;
import com.hazelcast.jet.impl.processor.PeekWrappedP;
import com.hazelcast.jet.impl.util.WrappingProcessorMetaSupplier;
import com.hazelcast.jet.impl.util.WrappingProcessorSupplier;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static com.hazelcast.jet.function.PredicateEx.alwaysTrue;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * Static utility class with factories of sinks and wrappers that log
 * the data flowing through the DAG. These processors are useful while
 * diagnosing the execution of Jet jobs. For other kinds of processors
 * refer to the {@link com.hazelcast.jet.core.processor package-level
 * documentation}.
 *
 * @since 3.0
 */
public final class DiagnosticProcessors {
    private DiagnosticProcessors() {
    }

    /**
     * Returns a meta-supplier of processors for a sink vertex that logs all
     * the data items it receives. The log category is {@code
     * com.hazelcast.jet.impl.processor.PeekWrappedP.<vertexName>#<processorIndex>}
     * and the level is INFO. {@link Watermark} items are always logged, but at
     * FINE level; they are <em>not</em> passed to {@code toStringFn}.
     * <p>
     * The vertex logs each item on whichever cluster member it happens to
     * receive it. Its primary purpose is for development, when running Jet on
     * a local machine.
     *
     * @param toStringFn a function that returns a string representation of a stream item
     * @param <T> stream item type
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier writeLoggerP(
            @Nonnull FunctionEx<T, ? extends CharSequence> toStringFn
    ) {
        checkSerializable(toStringFn, "toStringFn");

        return preferLocalParallelismOne(() -> new WriteLoggerP<>(toStringFn));
    }

    /**
     * Convenience for {@link #writeLoggerP(FunctionEx)} that uses
     * {@code toString()} as {@code toStringFn}.
     */
    @Nonnull
    public static ProcessorMetaSupplier writeLoggerP() {
        return writeLoggerP(Object::toString);
    }

    /**
     * Returns a meta-supplier that wraps the provided one and adds a logging
     * layer to each processor it creates. For each item the wrapped processor
     * removes from the inbox, the wrapping processor:
     * <ol><li>
     *     uses the {@code shouldLogFn} predicate to see whether to log the item
     * </li><li>
     *     if the item passed, uses {@code toStringFn} to get a string
     *     representation of the item
     * </li><li>
     *     logs the string at the INFO level, the logger is
     *     {@code com.hazelcast.jet.impl.processor.PeekWrappedP.<vertexName>#<processorIndex>}.
     *     The text is prefixed with "Input from X: ", where X is the edge
     *     ordinal the item is received from. Received watermarks are prefixed
     *     with just "Input: ".
     * </ol>
     * <p>
     * Note: Watermarks are always logged. {@link Watermark} objects are not
     * passed to {@code shouldLogFn} and {@code toStringFn}.
     *
     * @param toStringFn  a function that returns the string representation of the item.
     *                    You can use {@code Object::toString}.
     * @param shouldLogFn a function to filter the logged items. You can use {@link
     *                    PredicateEx#alwaysTrue()
     *                    alwaysTrue()} as a pass-through filter when you don't need any
     *                    filtering.
     * @param wrapped The wrapped meta-supplier.
     * @param <T> input item type
     *
     * @see #peekOutputP(FunctionEx, PredicateEx, ProcessorMetaSupplier)
     * @see #peekSnapshotP(FunctionEx, PredicateEx, ProcessorMetaSupplier)
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier peekInputP(
            @Nonnull FunctionEx<T, ? extends CharSequence> toStringFn,
            @Nonnull PredicateEx<T> shouldLogFn,
            @Nonnull ProcessorMetaSupplier wrapped
    ) {
        return new WrappingProcessorMetaSupplier(wrapped, p ->
                new PeekWrappedP<>(p, toStringFn, shouldLogFn, true, false, false));
    }

    /**
     * Same as {@link #peekInputP(FunctionEx, PredicateEx,
     * ProcessorMetaSupplier) peekInput(toStringFn, shouldLogFn, metaSupplier)},
     * but accepts a {@code ProcessorSupplier} instead of a meta-supplier.
     */
    @Nonnull
    public static <T> ProcessorSupplier peekInputP(
            @Nonnull FunctionEx<T, ? extends CharSequence> toStringFn,
            @Nonnull PredicateEx<T> shouldLogFn,
            @Nonnull ProcessorSupplier wrapped
    ) {
        return new WrappingProcessorSupplier(wrapped, p ->
                new PeekWrappedP<>(p, toStringFn, shouldLogFn, true, false, false));
    }

    /**
     * Same as {@link #peekInputP(FunctionEx, PredicateEx,
     * ProcessorMetaSupplier) peekInput(toStringFn, shouldLogFn, metaSupplier)},
     * but accepts a {@code SupplierEx} of processors instead of a
     * meta-supplier.
     */
    @Nonnull
    public static <T> SupplierEx<Processor> peekInputP(
            @Nonnull FunctionEx<T, ? extends CharSequence> toStringFn,
            @Nonnull PredicateEx<T> shouldLogFn,
            @Nonnull SupplierEx<Processor> wrapped
    ) {
        return () -> new PeekWrappedP<>(wrapped.get(), toStringFn, shouldLogFn, true, false, false);
    }

    /**
     * Convenience for {@link #peekInputP(FunctionEx,
     * PredicateEx, ProcessorMetaSupplier) peekInput(toStringFn,
     * shouldLogFn, metaSupplier)} with a pass-through filter and {@code
     * Object#toString} as the formatting function.
     */
    @Nonnull
    public static ProcessorMetaSupplier peekInputP(@Nonnull ProcessorMetaSupplier wrapped) {
        return peekInputP(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Convenience for {@link #peekInputP(FunctionEx,
     * PredicateEx, ProcessorMetaSupplier) peekInput(toStringFn,
     * shouldLogFn, metaSupplier)} with a pass-through filter and {@code
     * Object#toString} as the formatting function. This variant accepts a
     * {@code ProcessorSupplier} instead of a meta-supplier.
     */
    @Nonnull
    public static ProcessorSupplier peekInputP(@Nonnull ProcessorSupplier wrapped) {
        return peekInputP(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Convenience for {@link #peekInputP(FunctionEx,
     * PredicateEx, ProcessorMetaSupplier) peekInput(toStringFn,
     * shouldLogFn, metaSupplier)} with a pass-through filter and {@code
     * Object#toString} as the formatting function. This variant accepts a
     * {@code SupplierEx} of processors instead of a meta-supplier.
     */
    @Nonnull
    public static SupplierEx<Processor> peekInputP(@Nonnull SupplierEx<Processor> wrapped) {
        return peekInputP(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Returns a meta-supplier that wraps the provided one and adds a logging
     * layer to each processor it creates. For each item the wrapped processor
     * adds to the outbox, the wrapping processor:
     * <ol><li>
     *     uses the {@code shouldLogFn} predicate to see whether to log the item
     * </li><li>
     *     if the item passed, uses {@code toStringFn} to get a string
     *     representation of the item
     * </li><li>
     *     logs the string at the INFO level, the logger is
     *     {@code com.hazelcast.jet.impl.processor.PeekWrappedP.<vertexName>#<processorIndex>}.
     *     The logged text is prefixed with "Output to X: ", where X is the edge
     *     ordinal the item is sent to
     * </ol>
     * <p>
     * Technically speaking, snapshot data is emitted to the same outbox as regular
     * data, but this wrapper only logs the regular data. See {@link
     * #peekSnapshotP(FunctionEx, PredicateEx, ProcessorMetaSupplier)
     * peekSnapshot()}.
     *
     * <h4>Logging of Watermarks</h4>
     *
     * There are two kinds of watermarks:<ol>
     *     <li>Watermarks originated in the processor, prefixed in the logs
     *     with {@code "Output to N: "}
     *     <li>Watermarks received on input, which are forwarded automatically.
     *     These are prefixed with {@code "Output forwarded: "}
     * </ol>
     * Both are always logged. {@link Watermark} objects are not passed to
     * {@code shouldLogFn} or {@code toStringFn}.
     *
     * @param toStringFn  a function that returns the string representation of the item.
     *                    You can use {@code Object::toString}.
     * @param shouldLogFn a function to filter the logged items. You can use {@link
     *                    PredicateEx#alwaysTrue()
     *                    alwaysTrue()} as a pass-through filter when you don't need any
     *                    filtering.
     * @param wrapped The wrapped meta-supplier.
     * @param <T> output item type
     *
     * @see #peekInputP(FunctionEx, PredicateEx, ProcessorMetaSupplier)
     * @see #peekSnapshotP(FunctionEx, PredicateEx, ProcessorMetaSupplier)
     */
    @Nonnull
    public static <T> ProcessorMetaSupplier peekOutputP(
            @Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn,
            @Nonnull PredicateEx<? super T> shouldLogFn,
            @Nonnull ProcessorMetaSupplier wrapped
    ) {
        return new WrappingProcessorMetaSupplier(wrapped, p ->
                new PeekWrappedP<>(p, toStringFn, shouldLogFn, false, true, false));
    }

    /**
     * Same as {@link #peekOutputP(FunctionEx, PredicateEx,
     * ProcessorMetaSupplier) peekOutput(toStringFn, shouldLogFn, metaSupplier)},
     * but accepts a {@code ProcessorSupplier} instead of a meta-supplier.
     */
    @Nonnull
    public static <T> ProcessorSupplier peekOutputP(
            @Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn,
            @Nonnull PredicateEx<? super T> shouldLogFn,
            @Nonnull ProcessorSupplier wrapped
    ) {
        return new WrappingProcessorSupplier(wrapped, p ->
                new PeekWrappedP<>(p, toStringFn, shouldLogFn, false, true, false));
    }

    /**
     * Same as {@link #peekOutputP(FunctionEx, PredicateEx,
     * ProcessorMetaSupplier) peekOutput(toStringFn, shouldLogFn, metaSupplier)},
     * but accepts a {@code SupplierEx} of processors instead of a
     * meta-supplier.
     */
    @Nonnull
    public static <T> SupplierEx<Processor> peekOutputP(
            @Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn,
            @Nonnull PredicateEx<? super T> shouldLogFn,
            @Nonnull SupplierEx<Processor> wrapped) {
        return () -> new PeekWrappedP<>(wrapped.get(), toStringFn, shouldLogFn, false, true, false);
    }

    /**
     * Convenience for {@link #peekOutputP(FunctionEx,
     * PredicateEx, ProcessorMetaSupplier) peekOutput(toStringFn,
     * shouldLogFn, metaSupplier} with a pass-through filter and {@code
     * Object#toString} as the formatting function.
     */
    @Nonnull
    public static ProcessorMetaSupplier peekOutputP(@Nonnull ProcessorMetaSupplier wrapped) {
        return peekOutputP(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Convenience for {@link #peekOutputP(FunctionEx,
     * PredicateEx, ProcessorMetaSupplier) peekOutput(toStringFn,
     * shouldLogFn, metaSupplier} with a pass-through filter and {@code
     * Object#toString} as the formatting function. This variant accepts a
     * {@code ProcessorSupplier} instead of a meta-supplier.
     */
    @Nonnull
    public static ProcessorSupplier peekOutputP(@Nonnull ProcessorSupplier wrapped) {
        return peekOutputP(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Convenience for {@link #peekOutputP(FunctionEx,
     * PredicateEx, ProcessorMetaSupplier) peekOutput(toStringFn,
     * shouldLogFn, metaSupplier} with a pass-through filter and {@code
     * Object#toString} as the formatting function. This variant accepts a
     * {@code SupplierEx} of processors instead of a meta-supplier.
     */
    @Nonnull
    public static SupplierEx<Processor> peekOutputP(@Nonnull SupplierEx<Processor> wrapped) {
        return peekOutputP(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Returns a meta-supplier that wraps the provided one and adds a logging
     * layer to each processor it creates. For each item the wrapped processor
     * adds to the snapshot storage, the wrapping processor:
     * <ol><li>
     *     uses the {@code shouldLogFn} predicate to see whether to log the item
     * </li><li>
     *     if the item passed, uses {@code toStringFn} to get a string
     *     representation of the item
     * </li><li>
     *     logs the string at the INFO level, the category being
     *     {@code com.hazelcast.jet.impl.processor.PeekWrappedP.<vertexName>#<processorIndex>}
     * </ol>
     *
     * @param toStringFn  a function that returns the string representation of the item.
     *                    You can use {@code Object::toString}
     * @param shouldLogFn a function to filter the logged items. You can use {@link
     *                    PredicateEx#alwaysTrue()
     *                    alwaysTrue()} as a pass-through filter when you don't need any
     *                    filtering.
     * @param wrapped The wrapped meta-supplier.
     * @param <K> type of the key emitted to the snapshot
     * @param <V> type of the value emitted to the snapshot
     *
     * @see #peekInputP(FunctionEx, PredicateEx, ProcessorMetaSupplier)
     * @see #peekOutputP(FunctionEx, PredicateEx, ProcessorMetaSupplier)
     */
    @Nonnull
    public static <K, V> ProcessorMetaSupplier peekSnapshotP(
            @Nonnull FunctionEx<? super Entry<K, V>, ? extends CharSequence> toStringFn,
            @Nonnull PredicateEx<? super Entry<K, V>> shouldLogFn,
            @Nonnull ProcessorMetaSupplier wrapped
    ) {
        return new WrappingProcessorMetaSupplier(wrapped, p ->
                new PeekWrappedP<>(p, toStringFn, shouldLogFn, false, false, true));
    }

    /**
     * Same as {@link #peekSnapshotP(FunctionEx, PredicateEx,
     * ProcessorMetaSupplier) peekSnapshot(toStringFn, shouldLogFn, metaSupplier)},
     * but accepts a {@code ProcessorSupplier} instead of a meta-supplier.
     */
    @Nonnull
    public static <K, V> ProcessorSupplier peekSnapshotP(
            @Nonnull FunctionEx<? super Entry<K, V>, ? extends CharSequence> toStringFn,
            @Nonnull PredicateEx<? super Entry<K, V>> shouldLogFn,
            @Nonnull ProcessorSupplier wrapped
    ) {
        return new WrappingProcessorSupplier(wrapped, p ->
                new PeekWrappedP<>(p, toStringFn, shouldLogFn, false, false, true));
    }

    /**
     * Same as {@link #peekSnapshotP(FunctionEx, PredicateEx,
     * ProcessorMetaSupplier) peekSnapshot(toStringFn, shouldLogFn, metaSupplier)},
     * but accepts a {@code SupplierEx} of processors instead of a
     * meta-supplier.
     */
    @Nonnull
    public static <K, V> SupplierEx<Processor> peekSnapshotP(
            @Nonnull FunctionEx<? super Entry<K, V>, ? extends CharSequence> toStringFn,
            @Nonnull PredicateEx<? super Entry<K, V>> shouldLogFn,
            @Nonnull SupplierEx<Processor> wrapped) {
        return () -> new PeekWrappedP<>(wrapped.get(), toStringFn, shouldLogFn, false, false, true);
    }

    /**
     * Convenience for {@link #peekSnapshotP(FunctionEx,
     * PredicateEx, ProcessorMetaSupplier) peekSnapshot(toStringFn,
     * shouldLogFn, metaSupplier} with a pass-through filter and {@code
     * Object#toString} as the formatting function. This variant accepts a
     * {@code SupplierEx} of processors instead of a meta-supplier.
     */
    @Nonnull
    public static SupplierEx<Processor> peekSnapshotP(@Nonnull SupplierEx<Processor> wrapped) {
        return peekSnapshotP(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Convenience for {@link #peekSnapshotP(FunctionEx,
     * PredicateEx, ProcessorMetaSupplier) peekSnapshot(toStringFn,
     * shouldLogFn, metaSupplier} with a pass-through filter and {@code
     * Object#toString} as the formatting function.
     */
    @Nonnull
    public static ProcessorMetaSupplier peekSnapshotP(@Nonnull ProcessorMetaSupplier wrapped) {
        return peekSnapshotP(Object::toString, alwaysTrue(), wrapped);
    }

    /**
     * Convenience for {@link #peekSnapshotP(FunctionEx,
     * PredicateEx, ProcessorMetaSupplier) peekSnapshot(toStringFn,
     * shouldLogFn, metaSupplier} with a pass-through filter and {@code
     * Object#toString} as the formatting function. This variant accepts a
     * {@code ProcessorSupplier} instead of a meta-supplier.
     */
    @Nonnull
    public static ProcessorSupplier peekSnapshotP(@Nonnull ProcessorSupplier wrapped) {
        return peekSnapshotP(Object::toString, alwaysTrue(), wrapped);
    }
}
