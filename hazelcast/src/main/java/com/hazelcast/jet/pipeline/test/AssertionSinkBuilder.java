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

package com.hazelcast.jet.pipeline.test;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.pipeline.Sink;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.pipeline.SinkImpl.Type.TOTAL_PARALLELISM_ONE;
import static com.hazelcast.jet.impl.pipeline.test.AssertionP.assertionP;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * See {@link AssertionSinkBuilder#assertionSink(String, SupplierEx)}.
 *
 * @param <S> type of the state object
 * @param <T> type of the items the sink will accept
 *
 * @since Jet 3.2
 */
public final class AssertionSinkBuilder<S, T> {

    private final SupplierEx<? extends S> createFn;
    private final String name;
    private BiConsumerEx<? super S, ? super T> receiveFn;
    private ConsumerEx<? super S> timerFn = ConsumerEx.noop();
    private ConsumerEx<? super S> completeFn = ConsumerEx.noop();

    private AssertionSinkBuilder(
            @Nonnull String name,
            @Nonnull SupplierEx<? extends S> createFn
    ) {
        checkSerializable(createFn, "createFn");
        this.name = name;
        this.createFn = createFn;
    }

    /**
     * Returns a builder object that offers a step-by-step fluent API to build
     * an assertion {@link Sink} for the Pipeline API. An assertion sink is
     * typically used for testing of pipelines where you want to run
     * an assertion either on each item as they arrive, or when all items have been
     * received.
     * <p>
     * These are the callback functions you can provide to implement the sink's
     * behavior:
     * <ul><li>
     *     {@code createFn} creates the state which can be used to hold incoming
     *     items.
     * </li><li>
     *     {@code receiveFn} gets notified of each item the sink receives
     *     and can either assert the item directly or add it to the state
     *     object.
     * </li><li>
     *     {@code timerFn} is run periodically even when there are no items
     *     received. This can be used to assert that certain assertions have
     *     been reached within a specific time in streaming pipelines.
     * </li><li>
     *     {@code completeFn} is run after all the items have been received.
     *     This only applies to batch jobs, in a streaming job this method will
     *     never be called.
     * </li></ul>
     * The returned sink will have a global parallelism of 1: all items will be
     * sent to the same instance of the sink.
     * <p>
     * The sink doesn't participate in the fault-tolerance protocol, which
     * means you can't remember which items you already received across a job
     * restart.
     *
     * @param <S> type of the state object
     *
     * @since Jet 3.2
     */
    @Nonnull
    public static <S> AssertionSinkBuilder<S, Void> assertionSink(
            @Nonnull String name,
            @Nonnull SupplierEx<? extends S> createFn
    ) {
        return new AssertionSinkBuilder<>(name, createFn);
    }

    /**
     * Sets the function Jet will call upon receiving every item. The function
     * receives two arguments: the state object (as provided by the {@link
     * #createFn} and the received item. It may assert the item
     * directly or push it to the state object.
     *
     * @param receiveFn the function to execute upon receiving an item
     * @param <T_NEW> type of the items the sink will accept
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T_NEW> AssertionSinkBuilder<S, T_NEW> receiveFn(
            @Nonnull BiConsumerEx<? super S, ? super T_NEW> receiveFn
    ) {
        checkSerializable(receiveFn, "receiveFn");
        AssertionSinkBuilder<S, T_NEW> newThis = (AssertionSinkBuilder<S, T_NEW>) this;
        newThis.receiveFn = receiveFn;
        return newThis;
    }

    /**
     * Sets the function that will be called periodically. You can use this
     * function to assert that a condition will eventually be reached. The
     * function is guaranteed to be called even if there are no items coming
     * into the sink.
     * <p>
     * This function is optional.
     *
     * @param timerFn the optional "timer" function
     */
    @Nonnull
    public AssertionSinkBuilder<S, T> timerFn(@Nonnull ConsumerEx<? super S> timerFn) {
        checkSerializable(timerFn, "timerFn");
        this.timerFn = timerFn;
        return this;
    }

    /**
     * Sets the function that will be called after all the upstream stages have
     * completed and all the items were received.
     * <p>
     * This function is optional.
     *
     * @param completeFn the optional "complete" function
     */
    @Nonnull
    public AssertionSinkBuilder<S, T> completeFn(@Nonnull ConsumerEx<? super S> completeFn) {
        checkSerializable(completeFn, "completeFn");
        this.completeFn = completeFn;
        return this;
    }

    /**
     * Creates and returns the {@link Sink} with the components you supplied to
     * this builder.
     */
    @Nonnull
    public Sink<T> build() {
        Preconditions.checkNotNull(receiveFn, "receiveFn must be set");
        return new SinkImpl<>(name, assertionP(name, createFn, receiveFn, timerFn, completeFn), TOTAL_PARALLELISM_ONE);
    }
}
