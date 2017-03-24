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

import com.hazelcast.jet.Distributed.BiConsumer;
import com.hazelcast.jet.Distributed.Consumer;
import com.hazelcast.jet.Distributed.Supplier;
import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;

import javax.annotation.Nonnull;

public final class WriteBufferedP<B, T> implements Processor {

    private final B buffer;
    private final Consumer<B> bufferConsumer;
    private final BiConsumer<B, T> drainer;
    private final Consumer<B> bufferCloser;

    WriteBufferedP(Supplier<B> bufferSupplier,
                   BiConsumer<B, T> drainer,
                   Consumer<B> bufferConsumer,
                   Consumer<B> bufferCloser) {
        this.buffer = bufferSupplier.get();
        this.drainer = drainer;
        this.bufferConsumer = bufferConsumer;
        this.bufferCloser = bufferCloser;
    }

    public static <B, T> ProcessorSupplier writeBuffered(Supplier<B> bufferSupplier,
                                                         BiConsumer<B, T> drainer,
                                                         Consumer<B> bufferConsumer,
                                                         Consumer<B> bufferCloser) {
        return ProcessorSupplier.of(() -> new WriteBufferedP<>(bufferSupplier, drainer, bufferConsumer, bufferCloser));
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        inbox.drain((T t) -> drainer.accept(buffer, t));
        bufferConsumer.accept(buffer);
    }

    @Override
    public boolean complete() {
        bufferConsumer.accept(buffer);
        bufferCloser.accept(buffer);
        return true;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

}
