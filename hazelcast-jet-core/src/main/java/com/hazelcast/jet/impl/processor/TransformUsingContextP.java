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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.pipeline.ContextFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.jet.impl.processor.ProcessorSupplierWithContext.supplierWithContext;

/**
 * Backing processor for {@link
 * com.hazelcast.jet.pipeline.GeneralStage#mapUsingContext}.
 *
 * @param <C> context object type
 * @param <T> received item type
 * @param <R> emitted item type
 */
public final class TransformUsingContextP<C, T, R> extends AbstractProcessor {

    // package-visible for test
    C contextObject;

    private final ContextFactory<C> contextFactory;
    private final TriFunction<ResettableSingletonTraverser<R>, ? super C, ? super T,
                    ? extends Traverser<? extends R>> flatMapFn;

    private Traverser<? extends R> outputTraverser;
    private final ResettableSingletonTraverser<R> singletonTraverser = new ResettableSingletonTraverser<>();

    /**
     * Constructs a processor with the given mapping function.
     */
    private TransformUsingContextP(
            @Nonnull ContextFactory<C> contextFactory,
            @Nullable C contextObject,
            @Nonnull TriFunction<ResettableSingletonTraverser<R>, ? super C, ? super T,
                                    ? extends Traverser<? extends R>> flatMapFn
    ) {
        this.contextFactory = contextFactory;
        this.flatMapFn = flatMapFn;
        this.contextObject = contextObject;

        assert contextObject == null ^ contextFactory.hasLocalSharing()
                : "if contextObject is shared, it must be non-null, or vice versa";
    }

    @Override
    public boolean isCooperative() {
        return contextFactory.isCooperative();
    }

    @Override
    protected void init(@Nonnull Context context) {
        if (!contextFactory.hasLocalSharing()) {
            assert contextObject == null : "contextObject is not null: " + contextObject;
            contextObject = contextFactory.createFn().apply(context.jetInstance());
        }
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (outputTraverser == null) {
            outputTraverser = flatMapFn.apply(singletonTraverser, contextObject, (T) item);
        }
        if (emitFromTraverser(outputTraverser)) {
            outputTraverser = null;
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        // close() might be called even if init() was not called.
        // Only destroy the context if is not shared (i.e. it is our own).
        if (contextObject != null && !contextFactory.hasLocalSharing()) {
            contextFactory.destroyFn().accept(contextObject);
        }
        contextObject = null;
    }

    /**
     * The {@link ResettableSingletonTraverser} is passed as a first argument to
     * {@code flatMapFn}, it can be used if needed.
     */
    public static <C, T, R> ProcessorSupplier supplier(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<ResettableSingletonTraverser<R>, ? super C, ? super T,
                                            ? extends Traverser<? extends R>> flatMapFn
    ) {
        return supplierWithContext(contextFactory,
                (ctxF, ctxO) -> new TransformUsingContextP<C, T, R>(ctxF, ctxO, flatMapFn)
        );
    }
}
