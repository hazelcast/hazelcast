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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.function.DistributedTriFunction;
import com.hazelcast.jet.pipeline.ContextFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

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
    private final DistributedTriFunction<ResettableSingletonTraverser<R>, ? super C, ? super T,
            ? extends Traverser<? extends R>> flatMapFn;

    private Traverser<? extends R> outputTraverser;
    private final ResettableSingletonTraverser<R> singletonTraverser = new ResettableSingletonTraverser<>();

    /**
     * Constructs a processor with the given mapping function.
     */
    private TransformUsingContextP(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriFunction<ResettableSingletonTraverser<R>, ? super C, ? super T,
                    ? extends Traverser<? extends R>> flatMapFn,
            @Nullable C contextObject
    ) {
        this.contextFactory = contextFactory;
        this.flatMapFn = flatMapFn;
        this.contextObject = contextObject;

        assert contextObject == null ^ contextFactory.isSharedLocally()
                : "if contextObject is shared, it must be non-null, or vice versa";
    }

    @Override
    protected void init(@Nonnull Context context) {
        if (!contextFactory.isSharedLocally()) {
            assert contextObject == null : "contextObject is not null: " + contextObject;
            contextObject = contextFactory.createFn().apply(context.jetInstance());
        }
    }

    @Override
    public boolean isCooperative() {
        return contextFactory.isCooperative();
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
    public void close(@Nullable Throwable error) {
        // close() might be called even if init() was not called.
        // Only destroy the context if is not shared (i.e. it is our own).
        if (contextObject != null && !contextFactory.isSharedLocally()) {
            contextFactory.destroyFn().accept(contextObject);
        }
        contextObject = null;
    }

    private static final class Supplier<C, T, R> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final ContextFactory<C> contextFactory;
        private final DistributedTriFunction<ResettableSingletonTraverser<R>, ? super C, ? super T,
                ? extends Traverser<? extends R>> flatMapFn;
        private transient C contextObject;

        private Supplier(
                @Nonnull ContextFactory<C> contextFactory,
                @Nonnull DistributedTriFunction<ResettableSingletonTraverser<R>, ? super C, ? super T,
                        ? extends Traverser<? extends R>> flatMapFn
        ) {
            this.contextFactory = contextFactory;
            this.flatMapFn = flatMapFn;
        }

        @Override
        public void init(@Nonnull Context context) {
            if (contextFactory.isSharedLocally()) {
                contextObject = contextFactory.createFn().apply(context.jetInstance());
            }
        }

        @Nonnull @Override
        public Collection<? extends Processor> get(int count) {
            return Stream.generate(() -> new TransformUsingContextP<>(contextFactory, flatMapFn, contextObject))
                         .limit(count)
                         .collect(toList());
        }

        @Override
        public void close(Throwable error) {
            if (contextObject != null) {
                contextFactory.destroyFn().accept(contextObject);
            }
        }
    }

    /**
     * The {@link ResettableSingletonTraverser} is passed as a first argument to
     * {@code flatMapFn}, it can be used if needed.
     */
    public static <C, T, R> ProcessorSupplier supplier(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriFunction<ResettableSingletonTraverser<R>, ? super C, ? super T,
                    ? extends Traverser<? extends R>> flatMapFn
    ) {
        return new Supplier<>(contextFactory, flatMapFn);
    }
}
