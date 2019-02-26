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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.io.Serializable;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * A holder of functions needed to create and destroy a context object.
 * <p>
 * You can use the context factory from these Pipeline API methods:
 * <ul>
 *     <li>{@link GeneralStage#mapUsingContext}
 *     <li>{@link GeneralStage#filterUsingContext}
 *     <li>{@link GeneralStage#flatMapUsingContext}
 *     <li>{@link GeneralStage#mapUsingContextAsync}
 *     <li>{@link GeneralStage#filterUsingContextAsync}
 *     <li>{@link GeneralStage#flatMapUsingContextAsync}
 *     <li>{@link GeneralStageWithKey#mapUsingContext}
 *     <li>{@link GeneralStageWithKey#filterUsingContext}
 *     <li>{@link GeneralStageWithKey#flatMapUsingContext}
 *     <li>{@link GeneralStageWithKey#mapUsingContextAsync}
 *     <li>{@link GeneralStageWithKey#filterUsingContextAsync}
 *     <li>{@link GeneralStageWithKey#flatMapUsingContextAsync}
 * </ul>
 *
 * @param <C> the user-defined context object type
 */
public final class ContextFactory<C> implements Serializable {

    /**
     * Default value for {@link #maxPendingCallsPerProcessor}.
     */
    public static final int MAX_PENDING_CALLS_DEFAULT = 256;

    /**
     * Default value for {@link #isCooperative}.
     */
    public static final boolean COOPERATIVE_DEFAULT = true;

    /**
     * Default value for {@link #hasLocalSharing}.
     */
    public static final boolean SHARE_LOCALLY_DEFAULT = false;

    /**
     * Default value for {@link #hasOrderedAsyncResponses}.
     */
    public static final boolean ORDERED_ASYNC_RESPONSES_DEFAULT = true;

    private final DistributedFunction<JetInstance, ? extends C> createFn;
    private final DistributedConsumer<? super C> destroyFn;
    private final boolean isCooperative;
    private final boolean hasLocalSharing;
    private final int maxPendingCallsPerProcessor;
    private final boolean orderedAsyncResponses;

    private ContextFactory(
            DistributedFunction<JetInstance, ? extends C> createFn,
            DistributedConsumer<? super C> destroyFn,
            boolean isCooperative,
            boolean hasLocalSharing,
            int maxPendingCallsPerProcessor,
            boolean orderedAsyncResponses
    ) {
        this.createFn = createFn;
        this.destroyFn = destroyFn;
        this.isCooperative = isCooperative;
        this.hasLocalSharing = hasLocalSharing;
        this.maxPendingCallsPerProcessor = maxPendingCallsPerProcessor;
        this.orderedAsyncResponses = orderedAsyncResponses;
    }

    /**
     * Creates a new {@link ContextFactory} with the given create-function.
     *
     * @param createContextFn the function to create new context object, given
     *                        a JetInstance
     * @param <C> the user-defined context object type
     * @return a new factory instance
     */
    @Nonnull
    public static <C> ContextFactory<C> withCreateFn(
            @Nonnull DistributedFunction<JetInstance, ? extends C> createContextFn
    ) {
        checkSerializable(createContextFn, "createContextFn");
        return new ContextFactory<>(
                createContextFn, DistributedConsumer.noop(), COOPERATIVE_DEFAULT, SHARE_LOCALLY_DEFAULT,
                MAX_PENDING_CALLS_DEFAULT, ORDERED_ASYNC_RESPONSES_DEFAULT);
    }

    /**
     * Returns a copy of this {@link ContextFactory} with the destroy-function
     * replaced with the given function.
     * <p>
     * The destroy function is called at the end of the job to destroy all
     * created context objects.
     *
     * @param destroyFn the function to destroy user-defined context
     * @return a copy of this factory with the supplied destroy-function
     */
    @Nonnull
    public ContextFactory<C> withDestroyFn(@Nonnull DistributedConsumer<? super C> destroyFn) {
        checkSerializable(destroyFn, "destroyFn");
        return new ContextFactory<>(createFn, destroyFn, isCooperative, hasLocalSharing,
                maxPendingCallsPerProcessor, orderedAsyncResponses);
    }

    /**
     * Returns a copy of this {@link ContextFactory} with the
     * <em>isCooperative</em> flag set to {@code false}. The context factory is
     * cooperative by default. Call this method if your transform function
     * doesn't follow the {@linkplain Processor#isCooperative() cooperative
     * processor contract}, that is if it waits for IO, blocks for
     * synchronization, takes too long to complete etc. If you intend to use
     * the factory for an async operation, you also typically can use a
     * cooperative processor. Cooperative processors offer higher performance.
     *
     * @return a copy of this factory with the {@code isCooperative} flag set
     * to {@code false}.
     */
    @Nonnull
    public ContextFactory<C> toNonCooperative() {
        return new ContextFactory<>(createFn, destroyFn, false, hasLocalSharing,
                maxPendingCallsPerProcessor, orderedAsyncResponses);
    }

    /**
     * Returns a copy of this {@link ContextFactory} with the
     * <em>localSharing</em> flag set. If the pipeline doesn't have grouping,
     * there will be:
     * <ul>
     *     <li>one context object per local processor, if flag is disabled
     *     <li>one context object per member, if flag is enabled. Make
     *     sure the context object is <em>thread-safe</em> in this case.
     * </ul>
     *
     * @return a copy of this factory with the {@code hasLocalSharing} flag
     * set.
     */
    @Nonnull
    public ContextFactory<C> withLocalSharing() {
        return new ContextFactory<>(createFn, destroyFn, isCooperative, true,
                maxPendingCallsPerProcessor, orderedAsyncResponses);
    }

    /**
     * Returns a copy of this {@link ContextFactory} with the
     * <em>maxPendingCallsPerProcessor</em> property set to the given value. Jet
     * will execute at most this many concurrent async operations per processor
     * and will apply backpressure to the upstream.
     * <p>
     * If you use the same context factory on multiple pipeline stages, each
     * stage will count the pending calls independently.
     * <p>
     * This value is ignored when the {@code ContextFactory} is used in a
     * synchronous transformation.
     * <p>
     * Default value is {@value #MAX_PENDING_CALLS_DEFAULT}.
     *
     * @return a copy of this factory with the {@code maxPendingCallsPerProcessor}
     *      property set.
     */
    @Nonnull
    public ContextFactory<C> withMaxPendingCallsPerProcessor(int maxPendingCallsPerProcessor) {
        checkPositive(maxPendingCallsPerProcessor, "maxPendingCallsPerProcessor must be >= 1");
        return new ContextFactory<>(createFn, destroyFn, isCooperative, hasLocalSharing,
                maxPendingCallsPerProcessor, orderedAsyncResponses);
    }

    /**
     * Returns a copy of this {@link ContextFactory} with the
     * <em>unorderedAsyncResponses</em> flag set to true.
     * <p>
     * Jet can process asynchronous responses in two modes:
     * <ol><li>
     *     <b>Unordered:</b> results of the async calls are emitted as they
     *     arrive. This mode is enabled by this method.
     * </li><li>
     *     <b>Ordered:</b> results of the async calls are emitted in the submission
     *     order. This is the default.
     * </ol>
     * The unordered mode can be faster:
     * <ul><li>
     *     in the ordered mode, one stalling call will block all subsequent items,
     *     even though responses for them were already received
     * </li><li>
     *     to preserve the order after a restart, the ordered implementation when
     *     saving the state to the snapshot waits for all async calls to complete.
     *     This creates a hiccup depending on the async call latency. The unordered
     *     one saves in-flight items to the state snapshot.
     * </ul>
     * The order of watermarks is preserved even in the unordered mode. Jet
     * forwards the watermark after having emitted all the results of the items
     * that came before it. One stalling response will prevent a windowed
     * operation downstream from finishing, but if the operation is configured
     * to emit early results, they will be more correct with the unordered
     * approach.
     * <p>
     * This value is ignored when the {@code ContextFactory} is used in a
     * synchronous transformation: the output is always ordered in this case.
     *
     * @return a copy of this factory with the {@code unorderedAsyncResponses} flag set.
     */
    @Nonnull
    public ContextFactory<C> withUnorderedAsyncResponses() {
        return new ContextFactory<>(createFn, destroyFn, isCooperative, hasLocalSharing,
                maxPendingCallsPerProcessor, false);
    }

    /**
     * Returns the create-function.
     */
    @Nonnull
    public DistributedFunction<JetInstance, ? extends C> createFn() {
        return createFn;
    }

    /**
     * Returns the destroy-function.
     */
    @Nonnull
    public DistributedConsumer<? super C> destroyFn() {
        return destroyFn;
    }

    /**
     * Returns the {@code isCooperative} flag.
     */
    public boolean isCooperative() {
        return isCooperative;
    }

    /**
     * Returns the {@code hasLocalSharing} flag.
     */
    public boolean hasLocalSharing() {
        return hasLocalSharing;
    }

    /**
     * Returns the maximum pending calls per processor, see {@link
     * #withMaxPendingCallsPerProcessor(int)}.
     */
    public int maxPendingCallsPerProcessor() {
        return maxPendingCallsPerProcessor;
    }

    /**
     * Tells whether the async responses are ordered, see {@link
     * #withUnorderedAsyncResponses()}.
     */
    public boolean hasOrderedAsyncResponses() {
        return orderedAsyncResponses;
    }
}
