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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * A holder of functions needed to create and destroy a context object.
 * <p>
 * You can use the context factory from these Pipeline API methods:
 * <ul>
 *     <li>{@link GeneralStage#mapUsingContext}
 *     <li>{@link GeneralStage#filterUsingContext}
 *     <li>{@link GeneralStage#flatMapUsingContext}
 *     <li>{@link GeneralStageWithKey#mapUsingContext}
 *     <li>{@link GeneralStageWithKey#filterUsingContext}
 *     <li>{@link GeneralStageWithKey#flatMapUsingContext}
 * </ul>
 *
 * @param <C> the user-defined context object type
 */
public final class ContextFactory<C> implements Serializable {

    private static final boolean COOPERATIVE_DEFAULT = true;
    private static final boolean SHARE_LOCALLY_DEFAULT = false;

    private final DistributedFunction<JetInstance, ? extends C> createFn;
    private final DistributedConsumer<? super C> destroyFn;
    private final boolean isCooperative;
    private final boolean isSharedLocally;

    private ContextFactory(
            DistributedFunction<JetInstance, ? extends C> createFn,
            DistributedConsumer<? super C> destroyFn,
            boolean isCooperative,
            boolean isSharedLocally
    ) {
        this.createFn = createFn;
        this.destroyFn = destroyFn;
        this.isCooperative = isCooperative;
        this.isSharedLocally = isSharedLocally;
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
        return new ContextFactory<>(
                createContextFn, DistributedConsumer.noop(), COOPERATIVE_DEFAULT, SHARE_LOCALLY_DEFAULT);
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
        return new ContextFactory<>(createFn, destroyFn, isCooperative, isSharedLocally);
    }

    /**
     * Returns a copy of this {@link ContextFactory} with the
     * <em>isCooperative</em> flag set to {@code false}. The context factory is
     * cooperative by default. Call this method if your transform function
     * doesn't follow the {@linkplain Processor#isCooperative() cooperative
     * processor contract}.
     *
     * @return a copy of this factory with the {@code isCooperative} flag set
     * to {@code false}.
     */
    @Nonnull
    public ContextFactory<C> nonCooperative() {
        return new ContextFactory<>(createFn, destroyFn, false, isSharedLocally);
    }

    /**
     * Returns a copy of this {@link ContextFactory} with the
     * <em>shareLocally</em> flag set. If the pipeline doesn't have grouping,
     * there will be:
     * <ul>
     *     <li>one context object per local processor, if flag is disabled
     *     <li>one context object per member, if flag is enabled. Make
     *     sure the context object is <em>thread-safe</em> in this case.
     * </ul>
     * If the pipeline has grouping, the context object is never shared and
     * this flag is ignored.
     *
     * @return a copy of this factory with the {@code isSharedLocally} flag set.
     */
    @Nonnull
    public ContextFactory<C> shareLocally() {
        return new ContextFactory<>(createFn, destroyFn, isCooperative, true);
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
     * Returns the {@code isSharedLocally} flag.
     */
    public boolean isSharedLocally() {
        return isSharedLocally;
    }
}
