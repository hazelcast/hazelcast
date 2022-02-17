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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.processor.ProcessorSupplierWithService.supplierWithService;

/**
 * Backing processor for {@link
 * com.hazelcast.jet.pipeline.GeneralStage#mapUsingService}.
 *
 * @param <S> service type
 * @param <T> received item type
 * @param <R> emitted item type
 */
public final class TransformUsingServiceP<C, S, T, R> extends AbstractTransformUsingServiceP<C, S> {

    private final TriFunction<? super ResettableSingletonTraverser<R>, ? super S, ? super T, ? extends Traverser<R>>
            flatMapFn;

    private Traverser<? extends R> outputTraverser;
    private final ResettableSingletonTraverser<R> singletonTraverser = new ResettableSingletonTraverser<>();

    /**
     * Constructs a processor with the given mapping function.
     */
    private TransformUsingServiceP(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            @Nonnull C context,
            @Nonnull TriFunction<? super ResettableSingletonTraverser<R>, ? super S, ? super T, ? extends Traverser<R>>
                    flatMapFn
    ) {
        super(serviceFactory, context);
        this.flatMapFn = flatMapFn;
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

    /**
     * The {@link ResettableSingletonTraverser} is passed as a first argument to
     * {@code flatMapFn}, it can be used if needed.
     */
    public static <C, S, T, R> ProcessorSupplier supplier(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            @Nonnull TriFunction<? super ResettableSingletonTraverser<R>, ? super S, ? super T, ? extends Traverser<R>>
                    flatMapFn
    ) {
        return supplierWithService(serviceFactory,
                (serviceFn, context) -> new TransformUsingServiceP<C, S, T, R>(serviceFn, context, flatMapFn)
        );
    }
}
