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
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
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
 * @param <IR> intermediate result type
 * @param <R> emitted item type
 */
public class AsyncTransformUsingServiceOrderedP<C, S, T, IR, R> extends AbstractAsyncTransformUsingServiceP<C, S> {

    private final BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<IR>> callAsyncFn;
    private final BiFunctionEx<? super T, ? super IR, ? extends Traverser<? extends R>> mapResultFn;

    // The queue holds both watermarks and output items
    private ArrayDeque<Object> queue;
    // The number of watermarks in the queue
    private int queuedWmCount;

    private Traverser<?> currentTraverser = Traversers.empty();
    private final ResettableSingletonTraverser<Watermark> watermarkTraverser = new ResettableSingletonTraverser<>();

    @Probe(name = "numInFlightOps")
    private final Counter asyncOpsCounterMetric = SwCounter.newSwCounter();

    /**
     * Constructs a processor with the given mapping function.
     *
     * @param mapResultFn a function to map the intermediate result returned by
     *                    the future. One could think it's the same as CompletableFuture.thenApply(),
     *                    however, the {@code mapResultFn} is executed on the processor thread
     *                    and not concurrently, therefore the function can be stateful.
     */
    public AsyncTransformUsingServiceOrderedP(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            @Nullable C serviceContext,
            int maxConcurrentOps,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<IR>> callAsyncFn,
            @Nonnull BiFunctionEx<? super T, ? super IR, ? extends Traverser<? extends R>> mapResultFn
    ) {
        super(serviceFactory, serviceContext, maxConcurrentOps, true);
        this.callAsyncFn = callAsyncFn;
        this.mapResultFn = mapResultFn;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);
        // Size for the worst case: interleaved output items an WMs
        queue = new ArrayDeque<>(maxConcurrentOps * 2);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (isQueueFull() && !tryFlushQueue()) {
            return false;
        }
        @SuppressWarnings("unchecked")
        T castItem = (T) item;
        CompletableFuture<IR> future = callAsyncFn.apply(service, castItem);
        if (future != null) {
            queue.add(tuple2(castItem, future));
        }
        return true;
    }

    boolean isQueueFull() {
        return queue.size() - queuedWmCount == maxConcurrentOps;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        tryFlushQueue();
        if (queue.peekLast() instanceof Watermark) {
            // conflate the previous wm with the current one
            queue.removeLast();
            queue.add(watermark);
        } else {
            queue.add(watermark);
            queuedWmCount++;
        }
        return true;
    }

    @Override
    public boolean tryProcess() {
        tryFlushQueue();
        asyncOpsCounterMetric.set(queue.size());
        return true;
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
    boolean tryFlushQueue() {
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
                queuedWmCount--;
            } else {
                @SuppressWarnings("unchecked")
                Tuple2<T, CompletableFuture<IR>> cast = (Tuple2<T, CompletableFuture<IR>>) o;
                T item = cast.f0();
                CompletableFuture<IR> future = cast.f1();
                assert future != null;
                if (!future.isDone()) {
                    return false;
                }
                try {
                    currentTraverser = mapResultFn.apply(item, future.get());
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
    public static <C, S, T, R> ProcessorSupplier supplier(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            int maxConcurrentOps,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<Traverser<R>>> callAsyncFn
    ) {
        return supplierWithService(serviceFactory, (serviceFn, context) ->
                new AsyncTransformUsingServiceOrderedP<>(serviceFn, context, maxConcurrentOps, callAsyncFn, (i, r) -> r));
    }
}
