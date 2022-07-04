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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.processor.ProcessorSupplierWithService.supplierWithService;

/**
 * Processor which, for each received item, emits all the items from the
 * traverser returned by the given async item-to-traverser function, using a
 * service.
 * <p>
 * This processor keeps the order of input items: a stalling call for one item
 * will stall all subsequent items.
 *
 * @param <S> context object type
 * @param <T> received item type
 * @param <R> emitted item type
 */
public final class AsyncTransformUsingServiceBatchedP<C, S, T, R>
        extends AsyncTransformUsingServiceOrderedP<C, S, List<T>, Traverser<R>, R> {

    private final int maxBatchSize;

    /**
     * Constructs a processor with the given mapping function.
     */
    public AsyncTransformUsingServiceBatchedP(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            @Nullable C serviceContext,
            int maxConcurrentOps,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<Traverser<R>>> callAsyncFn
    ) {
        super(serviceFactory, serviceContext, maxConcurrentOps, callAsyncFn, (i, r) -> r);
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (isQueueFull() && !tryFlushQueue()) {
            return;
        }
        // put the inbox items into a list and pass to the superclass as a single item
        List<T> batch = new ArrayList<>(Math.min(inbox.size(), maxBatchSize));
        inbox.drainTo(batch, maxBatchSize);
        boolean success = super.tryProcess(ordinal, batch);
        assert success : "the superclass didn't handle the batch";
    }

    /**
     * The {@link ResettableSingletonTraverser} is passed as a first argument to
     * {@code callAsyncFn}, it can be used if needed.
     */
    public static <C, S, T, R> ProcessorSupplier supplier(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            int maxConcurrentOps,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<Traverser<R>>> callAsyncFn
    ) {
        return supplierWithService(serviceFactory, (factory, context) ->
                new AsyncTransformUsingServiceBatchedP<>(factory, context, maxConcurrentOps, maxBatchSize, callAsyncFn));
    }
}
