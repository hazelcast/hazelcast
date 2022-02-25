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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class AbstractAsyncTransformUsingServiceP<C, S> extends AbstractTransformUsingServiceP<C, S> {

    /**
     * Jet will execute at most this many concurrent async operations per processor
     * and will apply backpressure to the upstream to enforce it.
     * <p>
     * Default value is {@value GeneralStage#DEFAULT_MAX_CONCURRENT_OPS}.
     */
    protected final int maxConcurrentOps;

    /**
     * Jet can process asynchronous responses in two modes:
     * <ol><li>
     *     <b>Ordered:</b> results of the async calls are emitted in the submission
     *     order. This is the default.
     * <li>
     *     <b>Unordered:</b> results of the async calls are emitted as they
     *     arrive. This mode is enabled by this method.
     * </ol>
     * The unordered mode can be faster:
     * <ul><li>
     *     in the ordered mode, one stalling call will block all subsequent items,
     *     even though responses for them were already received
     * <li>
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
     */
    protected final boolean preserveOrder;

    public AbstractAsyncTransformUsingServiceP(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            @Nullable C serviceContext,
            int maxConcurrentOps,
            boolean preserveOrder
    ) {
        super(serviceFactory, serviceContext);
        this.maxConcurrentOps = maxConcurrentOps;
        this.preserveOrder = preserveOrder;
    }
}
