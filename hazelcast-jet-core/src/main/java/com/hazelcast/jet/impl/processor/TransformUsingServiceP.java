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
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.jet.impl.processor.ProcessorSupplierWithService.supplierWithService;

/**
 * Backing processor for {@link
 * com.hazelcast.jet.pipeline.GeneralStage#mapUsingService}.
 *
 * @param <S> service type
 * @param <T> received item type
 * @param <R> emitted item type
 */
public final class TransformUsingServiceP<S, T, R> extends AbstractProcessor {

    // package-visible for test
    S service;

    private final ServiceFactory<S> serviceFactory;
    private final TriFunction<ResettableSingletonTraverser<R>, ? super S, ? super T,
                    ? extends Traverser<? extends R>> flatMapFn;

    private Traverser<? extends R> outputTraverser;
    private final ResettableSingletonTraverser<R> singletonTraverser = new ResettableSingletonTraverser<>();

    /**
     * Constructs a processor with the given mapping function.
     */
    private TransformUsingServiceP(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nullable S service,
            @Nonnull TriFunction<ResettableSingletonTraverser<R>, ? super S, ? super T,
                                    ? extends Traverser<? extends R>> flatMapFn
    ) {
        this.serviceFactory = serviceFactory;
        this.flatMapFn = flatMapFn;
        this.service = service;

        assert service == null ^ serviceFactory.hasLocalSharing()
                : "if service is shared, it must be non-null, or vice versa";
    }

    @Override
    public boolean isCooperative() {
        return serviceFactory.isCooperative();
    }

    @Override
    protected void init(@Nonnull Context context) {
        if (!serviceFactory.hasLocalSharing()) {
            assert service == null : "service is not null: " + service;
            service = serviceFactory.createFn().apply(context.jetInstance());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (outputTraverser == null) {
            outputTraverser = flatMapFn.apply(singletonTraverser, service, (T) item);
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
        // Only destroy the service if is not shared (i.e. it is our own).
        if (service != null && !serviceFactory.hasLocalSharing()) {
            serviceFactory.destroyFn().accept(service);
        }
        service = null;
    }

    /**
     * The {@link ResettableSingletonTraverser} is passed as a first argument to
     * {@code flatMapFn}, it can be used if needed.
     */
    public static <S, T, R> ProcessorSupplier supplier(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull TriFunction<ResettableSingletonTraverser<R>, ? super S, ? super T,
                                            ? extends Traverser<? extends R>> flatMapFn
    ) {
        return supplierWithService(serviceFactory,
                (serviceFn, service) -> new TransformUsingServiceP<S, T, R>(serviceFn, service, flatMapFn)
        );
    }
}
