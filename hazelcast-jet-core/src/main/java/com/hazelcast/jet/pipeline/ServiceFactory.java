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

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Processor;

import javax.annotation.Nonnull;
import java.io.Serializable;

import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * A holder of functions needed to create and destroy a service object.
 * <p>
 * You can use the service factory from these Pipeline API methods:
 * <ul>
 *     <li>{@link GeneralStage#mapUsingService}
 *     <li>{@link GeneralStage#filterUsingService}
 *     <li>{@link GeneralStage#flatMapUsingService}
 *     <li>{@link GeneralStage#mapUsingServiceAsync}
 *     <li>{@link GeneralStage#filterUsingServiceAsync}
 *     <li>{@link GeneralStage#flatMapUsingServiceAsync}
 *     <li>{@link GeneralStageWithKey#mapUsingservice}
 *     <li>{@link GeneralStageWithKey#filterUsingservice}
 *     <li>{@link GeneralStageWithKey#flatMapUsingservice}
 *     <li>{@link GeneralStageWithKey#mapUsingserviceAsync}
 *     <li>{@link GeneralStageWithKey#filterUsingserviceAsync}
 *     <li>{@link GeneralStageWithKey#flatMapUsingserviceAsync}
 * </ul>
 *
 * @param <S> the user-defined service object type
 *
 * @since 3.0
 */
public final class ServiceFactory<S> implements Serializable {

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

    private final FunctionEx<JetInstance, ? extends S> createFn;
    private final ConsumerEx<? super S> destroyFn;
    private final boolean isCooperative;
    private final boolean hasLocalSharing;
    private final int maxPendingCallsPerProcessor;
    private final boolean orderedAsyncResponses;

    private ServiceFactory(
            FunctionEx<JetInstance, ? extends S> createFn,
            ConsumerEx<? super S> destroyFn,
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
     * Creates a new {@link ServiceFactory} with the given create-function.
     *
     * @param createServiceFn the function to create new service object, given
     *                        a JetInstance
     * @param <S> the user-defined service object type
     * @return a new factory instance
     *
     * @since 3.0
     */
    @Nonnull
    public static <S> ServiceFactory<S> withCreateFn(
            @Nonnull FunctionEx<JetInstance, ? extends S> createServiceFn
    ) {
        checkSerializable(createServiceFn, "createServiceFn");
        return new ServiceFactory<>(
                createServiceFn, ConsumerEx.noop(), COOPERATIVE_DEFAULT, SHARE_LOCALLY_DEFAULT,
                MAX_PENDING_CALLS_DEFAULT, ORDERED_ASYNC_RESPONSES_DEFAULT);
    }

    /**
     * Returns a copy of this {@link ServiceFactory} with the destroy-function
     * replaced with the given function.
     * <p>
     * The destroy function is called at the end of the job to destroy all
     * created service objects.
     *
     * @param destroyFn the function to destroy user-defined service
     * @return a copy of this factory with the supplied destroy-function
     */
    @Nonnull
    public ServiceFactory<S> withDestroyFn(@Nonnull ConsumerEx<? super S> destroyFn) {
        checkSerializable(destroyFn, "destroyFn");
        return new ServiceFactory<>(createFn, destroyFn, isCooperative, hasLocalSharing,
                maxPendingCallsPerProcessor, orderedAsyncResponses);
    }

    /**
     * Returns a copy of this {@link ServiceFactory} with the
     * <em>isCooperative</em> flag set to {@code false}. The service factory is
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
    public ServiceFactory<S> toNonCooperative() {
        return new ServiceFactory<>(createFn, destroyFn, false, hasLocalSharing,
                maxPendingCallsPerProcessor, orderedAsyncResponses);
    }

    /**
     * Returns a copy of this {@link ServiceFactory} with the
     * <em>localSharing</em> flag set. If the pipeline doesn't have grouping,
     * there will be:
     * <ul>
     *     <li>one service object per local processor, if flag is disabled
     *     <li>one service object per member, if flag is enabled. Make
     *     sure the service object is <em>thread-safe</em> in this case.
     * </ul>
     *
     * @return a copy of this factory with the {@code hasLocalSharing} flag
     * set.
     */
    @Nonnull
    public ServiceFactory<S> withLocalSharing() {
        return new ServiceFactory<>(createFn, destroyFn, isCooperative, true,
                maxPendingCallsPerProcessor, orderedAsyncResponses);
    }

    /**
     * Returns a copy of this {@link ServiceFactory} with the
     * <em>maxPendingCallsPerProcessor</em> property set to the given value. Jet
     * will execute at most this many concurrent async operations per processor
     * and will apply backpressure to the upstream.
     * <p>
     * If you use the same service factory on multiple pipeline stages, each
     * stage will count the pending calls independently.
     * <p>
     * This value is ignored when the {@code ServiceFactory} is used in a
     * synchronous transformation.
     * <p>
     * Default value is {@value #MAX_PENDING_CALLS_DEFAULT}.
     *
     * @return a copy of this factory with the {@code maxPendingCallsPerProcessor}
     *      property set.
     */
    @Nonnull
    public ServiceFactory<S> withMaxPendingCallsPerProcessor(int maxPendingCallsPerProcessor) {
        checkPositive(maxPendingCallsPerProcessor, "maxPendingCallsPerProcessor must be >= 1");
        return new ServiceFactory<>(createFn, destroyFn, isCooperative, hasLocalSharing,
                maxPendingCallsPerProcessor, orderedAsyncResponses);
    }

    /**
     * Returns a copy of this {@link ServiceFactory} with the
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
     * This value is ignored when the {@code ServiceFactory} is used in a
     * synchronous transformation: the output is always ordered in this case.
     *
     * @return a copy of this factory with the {@code unorderedAsyncResponses} flag set.
     */
    @Nonnull
    public ServiceFactory<S> withUnorderedAsyncResponses() {
        return new ServiceFactory<>(createFn, destroyFn, isCooperative, hasLocalSharing,
                maxPendingCallsPerProcessor, false);
    }

    /**
     * Returns the create-function.
     */
    @Nonnull
    public FunctionEx<JetInstance, ? extends S> createFn() {
        return createFn;
    }

    /**
     * Returns the destroy-function.
     */
    @Nonnull
    public ConsumerEx<? super S> destroyFn() {
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
