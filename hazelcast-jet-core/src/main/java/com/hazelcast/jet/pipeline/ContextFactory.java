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
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.io.Serializable;

import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;

/**
 * A holder of functions needed to create and destroy a context object that
 * can be used in the Pipeline API to give the transforming functions (map,
 * filter, flatMap) access to some resource that would be too expensive to
 * acquire-release on each function call. It also provides access to the
 * local {@code JetInstance}. Among others, this gives you the ability to
 * interact with IMDG data structures such as {@code IMap} and {@code
 * ReplicatedMap} during the transformation.
 * <p>
 * You can use the context factory from these Pipeline API methods:
 * <ul>
 *     <li>{@link GeneralStage#mapUsingContext}
 *     <li>{@link GeneralStage#filterUsingContext}
 *     <li>{@link GeneralStage#flatMapUsingContext}
 * </ul>
 * To get a context factory, choose one of the predefined factories in
 * {@link ContextFactories} or create your own using the builder you get
 * by calling {@link #withCreateFn}. The factory instances must be
 * immutable.
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
     * @param createContextFn the function to create new context object, given a JetInstance
     * @param <C> the user-defined context object type
     * @return a new factory instance
     */
    @Nonnull
    public static <C> ContextFactory<C> withCreateFn(
            @Nonnull DistributedFunction<JetInstance, ? extends C> createContextFn
    ) {
        return new ContextFactory<>(createContextFn, noopConsumer(), COOPERATIVE_DEFAULT, SHARE_LOCALLY_DEFAULT);
    }

    /**
     * Returns a copy of this {@link ContextFactory} with the destory-function
     * replaced with the given function.
     *
     * @param destroyFn the function to destroy user-defined context. It will be called
     *                  when the job finishes
     * @return a copy of this factory with the supplied destroy-function
     */
    @Nonnull
    public ContextFactory<C> withDestroyFn(@Nonnull DistributedConsumer<? super C> destroyFn) {
        return new ContextFactory<>(createFn, destroyFn, isCooperative, isSharedLocally);
    }

    /**
     * Returns a copy of this {@link ContextFactory} with the <em>isCooperative</em>
     * flag set to {@code false}. The context factory is cooperative by default.
     * Call this method if any of the calls to the methods in the context may block
     * or otherwise take long to complete.
     * <p>
     * The contract of <em>cooperative multithreading</em> is described {@link
     * com.hazelcast.jet.core.Processor#isCooperative() here}.
     *
     * @return a copy of this factory with the {@code isCooperative} flag set to {@code false}.
     */
    @Nonnull
    public ContextFactory<C> nonCooperative() {
        return new ContextFactory<>(createFn, destroyFn, false, isSharedLocally);
    }

    /**
     * Returns a copy of this {@link ContextFactory} with the <em>shareLocally</em>
     * flag set to {@code true}. By default the context object is not shared so each
     * parallel processor gets its own instance. If you enable sharing, the context
     * object will be used from multiple threads &mdash; make sure that it is
     * <em>thread-safe</em>.
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
