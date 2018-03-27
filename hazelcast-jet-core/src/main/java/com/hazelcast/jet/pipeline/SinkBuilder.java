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
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;
import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;

/**
 * Offers a step-by-step fluent API to build a custom {@link Sink} for the
 * Pipeline API. It allows you to keep a single-threaded, stateful writer
 * object in each instance of a Jet worker dedicated to driving the sink.
 * Its primary intended purpose is to serve as the holder of references to
 * external resources and optional buffers. These are the callback
 * functions you can provide to implement the sink's behavior:
 * <ol><li>
 *     {@code createFn} creates the writer. Gets the local Jet instance as
 *     argument. This component is required.
 * </li><li>
 *     {@code onReceiveFn} gets notified of each item the sink receives and
 *     (typically) passes it to the writer. This component is required.
 * </li><li>
 *     {@code flushFn} flushes the writer. This component is optional.
 * </li><li>
 *     {@code destroyFn} destroys the writer. This component is optional.
 * </li></ol>
 *
 * @param <W> type of the writer object
 * @param <T> type of the items the sink will accept
 */
public final class SinkBuilder<W, T> {

    private final DistributedFunction<? super JetInstance, ? extends W> createFn;
    private DistributedBiConsumer<? super W, ? super T> onReceiveFn;
    private DistributedConsumer<? super W> flushFn = noopConsumer();
    private DistributedConsumer<? super W> destroyFn = noopConsumer();

    SinkBuilder(@Nonnull DistributedFunction<? super JetInstance, ? extends W> createFn) {
        this.createFn = createFn;
    }

    /**
     * Sets the function Jet will call upon receiving an item. The function
     * receives two arguments: the writer object (as provided by the {@link
     * #createFn} and the received item. Its job is to push the item to the
     * writer.
     *
     * @param onReceiveFn the "add item to the writer" function
     */
    @Nonnull
    public SinkBuilder<W, T> onReceiveFn(@Nonnull DistributedBiConsumer<? super W, ? super T> onReceiveFn) {
        this.onReceiveFn = onReceiveFn;
        return this;
    }

    /**
     * Sets the function that implements the sink's flushing behavior. If your
     * writer is buffered, instead of relying on some automatic flushing policy
     * you can provide this function so Jet can choose the best moment to
     * flush.
     * <p>
     * You are not required to provide this function in case your implementation
     * doesn't need it.
     *
     * @param flushFn the optional "flush the writer" function
     */
    @Nonnull
    public SinkBuilder<W, T> flushFn(@Nonnull DistributedConsumer<? super W> flushFn) {
        this.flushFn = flushFn;
        return this;
    }

    /**
     * Sets the function that will destroy the writer and perform any cleanup. The
     * function is called when the job has been completed or cancelled. Jet guarantees
     * that no new items will be received in between the last call to {@code flushFn}
     * and the call to {@code destroyFn}.
     * <p>
     * You are not required to provide this function in case your implementation
     * doesn't need it.
     *
     * @param destroyFn the optional "destroy the writer" function
     */
    @Nonnull
    public SinkBuilder<W, T> destroyFn(@Nonnull DistributedConsumer<? super W> destroyFn) {
        this.destroyFn = destroyFn;
        return this;
    }

    /**
     * Creates and returns the {@link Sink} with the components you supplied to
     * this builder.
     */
    @Nonnull
    public Sink<T> build() {
        Preconditions.checkNotNull(onReceiveFn, "onReceiveFn must be set");

        // local copy for serialization
        DistributedFunction<? super JetInstance, ? extends W> createFn = this.createFn;
        DistributedSupplier<Processor> supplier = SinkProcessors.writeBufferedP(
                ctx -> createFn.apply(ctx.jetInstance()),
                onReceiveFn,
                flushFn,
                destroyFn
        );
        return new SinkImpl<>("custom-sink", preferLocalParallelismOne(supplier));
    }
}
