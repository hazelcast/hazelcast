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

package com.hazelcast.jet.core.processor;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.connector.WriteLoggerP;
import com.hazelcast.jet.impl.processor.PeekWrappedP;
import com.hazelcast.jet.impl.util.WrappingProcessorMetaSupplier;
import com.hazelcast.jet.impl.util.WrappingProcessorSupplier;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Objects;

import static com.hazelcast.function.PredicateEx.alwaysTrue;
import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * Static utility class with factories of sinks and wrappers that log
 * the data flowing through the DAG. These processors are useful while
 * diagnosing the execution of Jet jobs. For other kinds of processors
 * refer to the {@link com.hazelcast.jet.core.processor package-level
 * documentation}.
 *
 * @since Jet 3.0
 */
public final class DiagnosticProcessors {

    /**
     * A function that uses `Object.toString()` for non-arrays,
     * `Arrays.toString()` for arrays of primitive types and
     * `Arrays.deepToString()` for `Object[]`. Used for `peek()`
     * function in the DAG and Pipeline API.
     *
     * @since 5.1
     */
    @SuppressWarnings("checkstyle:ReturnCount")
    public static final FunctionEx<Object, String> PEEK_DEFAULT_TO_STRING = o -> {
        if (o instanceof Object[]) {
            return Arrays.deepToString((Object[]) o);
        } else if (o instanceof byte[]) {
            return Arrays.toString((byte[]) o);
        } else if (o instanceof short[]) {
            return Arrays.toString((short[]) o);
        } else if (o instanceof int[]) {
            return Arrays.toString((int[]) o);
        } else if (o instanceof long[]) {
            return Arrays.toString((long[]) o);
        } else if (o instanceof float[]) {
            return Arrays.toString((float[]) o);
        } else if (o instanceof double[]) {
            return Arrays.toString((double[]) o);
        } else if (o instanceof boolean[]) {
            return Arrays.toString((boolean[]) o);
        } else {
            return Objects.toString(o);
        }
    };

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
        return writeLoggerP(PEEK_DEFAULT_TO_STRING);
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
     *                    You can use {@link #PEEK_DEFAULT_TO_STRING}.
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
        return peekInputP(PEEK_DEFAULT_TO_STRING, alwaysTrue(), wrapped);
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
        return peekInputP(PEEK_DEFAULT_TO_STRING, alwaysTrue(), wrapped);
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
        return peekInputP(PEEK_DEFAULT_TO_STRING, alwaysTrue(), wrapped);
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
     *                    You can use {@link #PEEK_DEFAULT_TO_STRING}.
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
        return peekOutputP(PEEK_DEFAULT_TO_STRING, alwaysTrue(), wrapped);
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
        return peekOutputP(PEEK_DEFAULT_TO_STRING, alwaysTrue(), wrapped);
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
        return peekOutputP(PEEK_DEFAULT_TO_STRING, alwaysTrue(), wrapped);
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
     *                    You can use {@code DEFAULT_TO_STRING}
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
        return peekSnapshotP(PEEK_DEFAULT_TO_STRING, alwaysTrue(), wrapped);
    }

    /**
     * Convenience for {@link #peekSnapshotP(FunctionEx,
     * PredicateEx, ProcessorMetaSupplier) peekSnapshot(toStringFn,
     * shouldLogFn, metaSupplier} with a pass-through filter and {@code
     * Object#toString} as the formatting function.
     */
    @Nonnull
    public static ProcessorMetaSupplier peekSnapshotP(@Nonnull ProcessorMetaSupplier wrapped) {
        return peekSnapshotP(PEEK_DEFAULT_TO_STRING, alwaysTrue(), wrapped);
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
        return peekSnapshotP(PEEK_DEFAULT_TO_STRING, alwaysTrue(), wrapped);
    }
}
