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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.pipeline.ContextFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.processor.ProcessorSupplierWithContext.supplierWithContext;

/**
 * Processor which, for each received item, emits all the items from the
 * traverser returned by the given async item-to-traverser function, using a
 * context object.
 * <p>
 * This processor keeps the order of input items: a stalling call for one item
 * will stall all subsequent items.
 *
 * @param <C> context object type
 * @param <T> received item type
 * @param <R> emitted item type
 */
public final class AsyncTransformUsingContextOrderedP<C, T, R> extends AbstractProcessor {

    private final ContextFactory<C> contextFactory;
    private final DistributedBiFunction<? super C, ? super T, CompletableFuture<Traverser<R>>> callAsyncFn;

    private C contextObject;
    // on the queue there is either:
    // - tuple2(originalItem, future)
    // - watermark
    private ArrayDeque<Object> queue;
    private Traverser<?> currentTraverser = Traversers.empty();
    private int maxAsyncOps;
    private ResettableSingletonTraverser<Watermark> watermarkTraverser = new ResettableSingletonTraverser<>();
    private boolean tryProcessSucceeded;

    @Probe(name = "numInFlightOps")
    private final AtomicInteger asyncOpsCounterMetric = new AtomicInteger();

    /**
     * Constructs a processor with the given mapping function.
     */
    private AsyncTransformUsingContextOrderedP(
            @Nonnull ContextFactory<C> contextFactory,
            @Nullable C contextObject,
            @Nonnull DistributedBiFunction<? super C, ? super T, CompletableFuture<Traverser<R>>> callAsyncFn
    ) {
        this.contextFactory = contextFactory;
        this.callAsyncFn = callAsyncFn;
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
        maxAsyncOps = contextFactory.maxPendingCallsPerProcessor();
        queue = new ArrayDeque<>(maxAsyncOps);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (queue.size() == maxAsyncOps) {
            // if queue is full, try to emit and apply backpressure
            tryFlushQueue();
            return false;
        }
        @SuppressWarnings("unchecked")
        T castedItem = (T) item;
        CompletableFuture<? extends Traverser<R>> future = callAsyncFn.apply(contextObject, castedItem);
        if (future != null) {
            queue.add(tuple2(castedItem, future));
        }
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        tryFlushQueue();
        return queue.size() < maxAsyncOps
                && !getOutbox().hasUnfinishedItem()
                && queue.add(watermark);
    }

    @Override
    public boolean tryProcess() {
        if (tryProcessSucceeded) {
            tryFlushQueue();
        } else {
            emitFromTraverser(currentTraverser);
        }
        asyncOpsCounterMetric.lazySet(queue.size());
        return tryProcessSucceeded = !getOutbox().hasUnfinishedItem();
    }

    @Override
    public boolean complete() {
        return tryFlushQueue();
    }

    @Override
    public boolean saveToSnapshot() {
        // We're stateless, wait until responses to all async requests are emitted. This is a
        // stop-the-world situation, no new async requests are sent while waiting. If async requests
        // are slow, this might be a major slowdown.
        return tryFlushQueue();
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
     * Drains items from the queue until either:
     * <ul><li>
     *     encountering a non-completed item
     * </li><li>
     *     the outbox gets full
     * </li></ul>
     *
     * @return true if there are no more in-flight items and everything was emitted
     *         to the outbox
     */
    private boolean tryFlushQueue() {
        // We check the futures in submission order. While this might increase latency for some
        // later-submitted item that gets the result before some earlier-submitted one, we don't
        // have to do many volatile reads to check all the futures in each call or a concurrent
        // queue. It also doesn't shuffle the stream items.
        for (;;) {
            if (!emitFromTraverser(currentTraverser)) {
                return false;
            }
            Object o = queue.peek();
            if (o == null) {
                return true;
            }
            if (o instanceof Watermark) {
                watermarkTraverser.accept((Watermark) o);
                currentTraverser = watermarkTraverser;
            } else {
                @SuppressWarnings("unchecked")
                CompletableFuture<Traverser<R>> f = ((Tuple2<T, CompletableFuture<Traverser<R>>>) o).f1();
                if (!f.isDone()) {
                    return false;
                }
                try {
                    currentTraverser = f.get();
                    if (currentTraverser == null) {
                        currentTraverser = Traversers.empty();
                    }
                } catch (Throwable e) {
                    throw new JetException("Async operation completed exceptionally: " + e, e);
                }
            }
            queue.remove();
        }
    }

    /**
     * The {@link ResettableSingletonTraverser} is passed as a first argument to
     * {@code callAsyncFn}, it can be used if needed.
     */
    public static <C, T, R> ProcessorSupplier supplier(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, CompletableFuture<Traverser<R>>> callAsyncFn
    ) {
        return supplierWithContext(contextFactory,
                (ctxF, ctxO) -> new AsyncTransformUsingContextOrderedP<>(ctxF, ctxO, callAsyncFn)
        );
    }
}
