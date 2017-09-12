/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedIntFunction;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public final class WriteBufferedP<B, T> implements Processor {

    private final DistributedIntFunction<B> newBufferFn;
    private final DistributedBiConsumer<B, T> addToBufferFn;
    private final DistributedConsumer<B> flushBufferFn;
    private final DistributedConsumer<B> disposeBufferFn;

    private B buffer;

    WriteBufferedP(DistributedIntFunction<B> newBufferFn,
                   DistributedBiConsumer<B, T> addToBufferFn,
                   DistributedConsumer<B> flushBufferFn,
                   DistributedConsumer<B> disposeBufferFn
    ) {
        this.newBufferFn = newBufferFn;
        this.addToBufferFn = addToBufferFn;
        this.flushBufferFn = flushBufferFn;
        this.disposeBufferFn = disposeBufferFn;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        this.buffer = newBufferFn.apply(context.globalProcessorIndex());
    }

    /**
     * This is private API. Call
     * {@link com.hazelcast.jet.processor.SinkProcessors#writeBuffered(
     *        DistributedIntFunction, DistributedBiConsumer,
     *        DistributedConsumer, DistributedConsumer)
     * SinkProcessors.writeBuffered()} instead.
     */
    @Nonnull
    public static <B, T> ProcessorSupplier supplier(
            DistributedIntFunction<B> newBufferFn,
            DistributedBiConsumer<B, T> addToBufferFn,
            DistributedConsumer<B> flushBufferFn,
            DistributedConsumer<B> disposeBufferFn
    ) {
        return new ProcessorSupplier() {
            private transient List<WriteBufferedP<B, T>> processors;

            @Nonnull @Override
            public Collection<? extends Processor> get(int count) {
                return processors = IntStream
                        .range(0, count)
                        .mapToObj(i -> new WriteBufferedP<>(newBufferFn, addToBufferFn, flushBufferFn, disposeBufferFn))
                        .collect(toList());
            }

            @Override
            public void complete(Throwable error) {
                if (processors == null) {
                    return;
                }
                for (WriteBufferedP<B, T> p : processors) {
                    p.close();
                }
            }
        };
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        inbox.drain(item -> {
            if (!(item instanceof Watermark)) {
                addToBufferFn.accept(buffer, (T) item);
            }
        });
        flushBufferFn.accept(buffer);
    }

    @Override
    public boolean complete() {
        close();
        return true;
    }

    public void close() {
        disposeBufferFn.accept(buffer);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

}
