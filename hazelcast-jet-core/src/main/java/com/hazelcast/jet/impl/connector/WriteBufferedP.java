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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.function.ConsumerEx;
import com.hazelcast.jet.function.SupplierEx;

import javax.annotation.Nonnull;
import java.util.function.Consumer;

public final class WriteBufferedP<B, T> implements Processor {

    private final FunctionEx<? super Context, B> createFn;
    private final ConsumerEx<? super B> flushFn;
    private final ConsumerEx<? super B> destroyFn;

    private B buffer;
    private final Consumer<Object> inboxConsumer;

    WriteBufferedP(
            @Nonnull FunctionEx<? super Context, B> createFn,
            @Nonnull BiConsumerEx<? super B, ? super T> onReceiveFn,
            @Nonnull ConsumerEx<? super B> flushFn,
            @Nonnull ConsumerEx<? super B> destroyFn
    ) {
        this.createFn = createFn;
        this.flushFn = flushFn;
        this.destroyFn = destroyFn;

        inboxConsumer = item -> onReceiveFn.accept(buffer, (T) item);
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        buffer = createFn.apply(context);
        if (buffer == null) {
            throw new JetException("Null buffer created");
        }
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        inbox.drain(inboxConsumer);
        flushFn.accept(buffer);
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        // we're a sink, no need to forward the watermarks
        return true;
    }

    @Override
    public boolean complete() {
        return true;
    }

    @Override
    public void close() {
        if (buffer != null) {
            destroyFn.accept(buffer);
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    /**
     * This is private API. Call {@link SinkProcessors#writeBufferedP} instead.
     */
    @Nonnull
    public static <B, T> SupplierEx<Processor> supplier(
            @Nonnull FunctionEx<? super Context, ? extends B> createFn,
            @Nonnull BiConsumerEx<? super B, ? super T> onReceiveFn,
            @Nonnull ConsumerEx<? super B> flushFn,
            @Nonnull ConsumerEx<? super B> destroyFn
    ) {
        return () -> new WriteBufferedP<>(createFn, onReceiveFn, flushFn, destroyFn);
    }
}
