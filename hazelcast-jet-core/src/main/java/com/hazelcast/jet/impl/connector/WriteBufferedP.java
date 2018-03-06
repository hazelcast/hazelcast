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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.core.CloseableProcessorSupplier;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.io.Closeable;

public final class WriteBufferedP<B, T> implements Processor, Closeable {

    private final DistributedFunction<? super Context, B> createFn;
    private final DistributedBiConsumer<? super B, ? super T> onReceiveFn;
    private final DistributedConsumer<? super B> flushFn;
    private final DistributedConsumer<? super B> destroyFn;

    private B buffer;

    WriteBufferedP(
            @Nonnull DistributedFunction<? super Context, B> createFn,
            @Nonnull DistributedBiConsumer<? super B, ? super T> onReceiveFn,
            @Nonnull DistributedConsumer<? super B> flushFn,
            @Nonnull DistributedConsumer<? super B> destroyFn
    ) {
        this.createFn = createFn;
        this.onReceiveFn = onReceiveFn;
        this.flushFn = flushFn;
        this.destroyFn = destroyFn;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        buffer = createFn.apply(context);
    }

    /**
     * This is private API. Call
     * {@link com.hazelcast.jet.core.processor.SinkProcessors#writeBufferedP
     * SinkProcessors.writeBuffered()} instead.
     */
    @Nonnull
    public static <B, T> ProcessorSupplier supplier(
            @Nonnull DistributedFunction<? super Context, ? extends B> createFn,
            @Nonnull DistributedBiConsumer<? super B, ? super T> onReceiveFn,
            @Nonnull DistributedConsumer<? super B> flushFn,
            @Nonnull DistributedConsumer<? super B> destroyFn
    ) {
        return CloseableProcessorSupplier.of(
                () -> new WriteBufferedP<>(createFn, onReceiveFn, flushFn, destroyFn)
        );
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        inbox.drain(item -> onReceiveFn.accept(buffer, (T) item));
        flushFn.accept(buffer);
    }

    @Override
    public boolean complete() {
        close();
        return true;
    }

    public void close() {
        destroyFn.accept(buffer);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

}
