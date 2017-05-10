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

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedConsumer;
import com.hazelcast.jet.function.DistributedIntFunction;
import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;

public final class WriteBufferedP<B, T> implements Processor {

    private final DistributedConsumer<B> flushBuffer;
    private final DistributedIntFunction<B> newBuffer;
    private final DistributedBiConsumer<B, T> addToBuffer;
    private final DistributedConsumer<B> disposeBuffer;
    private B buffer;

    WriteBufferedP(DistributedIntFunction<B> newBuffer,
                   DistributedBiConsumer<B, T> addToBuffer,
                   DistributedConsumer<B> flushBuffer,
                   DistributedConsumer<B> disposeBuffer) {
        this.newBuffer = newBuffer;
        this.addToBuffer = addToBuffer;
        this.flushBuffer = flushBuffer;
        this.disposeBuffer = disposeBuffer;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        this.buffer = newBuffer.apply(context.globalProcessorIndex());
    }

    @Nonnull
    public static <B, T> DistributedSupplier<Processor> writeBuffered(
            DistributedIntFunction<B> newBuffer,
            DistributedBiConsumer<B, T> addToBuffer,
            DistributedConsumer<B> consumeBuffer,
            DistributedConsumer<B> closeBuffer
    ) {
        return () -> new WriteBufferedP<>(newBuffer, addToBuffer, consumeBuffer, closeBuffer);
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        inbox.drain(item -> {
            if (!(item instanceof Punctuation)) {
                addToBuffer.accept(buffer, (T) item);
            }
        });
        flushBuffer.accept(buffer);
    }

    @Override
    public boolean complete() {
        flushBuffer.accept(buffer);
        disposeBuffer.accept(buffer);
        return true;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

}
