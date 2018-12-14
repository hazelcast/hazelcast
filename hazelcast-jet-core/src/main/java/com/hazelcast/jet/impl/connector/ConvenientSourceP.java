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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.WatermarkSourceUtil;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.datamodel.TimestampedItem;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Implements a data source the user created using the Source Builder API.
 *
 * @see SourceProcessors#convenientSourceP
 * @see SourceProcessors#convenientTimestampedSourceP
 */
public class ConvenientSourceP<S, T> extends AbstractProcessor {

    /**
     * This processor's view of the buffer accessible to the user. Abstracts
     * away the difference between the plain and the timestamped buffer.
     */
    public interface SourceBufferConsumerSide<T> {
        /**
         * Returns a traverser over the contents of the buffer. Traversing the
         * items automatically removes them from the buffer.
         */
        Traverser<T> traverse();
        boolean isEmpty();
        boolean isClosed();
    }

    private final Function<? super Context, ? extends S> createFn;
    private final BiConsumer<? super S, ? super SourceBufferConsumerSide<?>> fillBufferFn;
    private final Consumer<? super S> destroyFn;
    private final SourceBufferConsumerSide<?> buffer;
    private final WatermarkSourceUtil<T> wsu;

    private boolean initialized;
    private S src;
    private Traverser<?> traverser;

    public ConvenientSourceP(
            @Nonnull Function<? super Context, ? extends S> createFn,
            @Nonnull BiConsumer<? super S, ? super SourceBufferConsumerSide<?>> fillBufferFn,
            @Nonnull Consumer<? super S> destroyFn,
            @Nonnull SourceBufferConsumerSide<?> buffer,
            @Nullable EventTimePolicy<? super T> eventTimePolicy
    ) {
        this.createFn = createFn;
        this.fillBufferFn = fillBufferFn;
        this.destroyFn = destroyFn;
        this.buffer = buffer;
        if (eventTimePolicy != null) {
            this.wsu = new WatermarkSourceUtil<>(eventTimePolicy);
            wsu.increasePartitionCount(1);
        } else {
            this.wsu = null;
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected void init(@Nonnull Context context) {
        src = createFn.apply(context);
        // createFn is allowed to return null, that's why we need this flag:
        initialized = true;
    }

    @Override
    public boolean complete() {
        if (traverser == null) {
            fillBufferFn.accept(src, buffer);
            traverser =
                    wsu == null ? buffer.traverse()
                    : buffer.isEmpty() ? wsu.handleNoEvent()
                    : buffer.traverse().flatMap(t -> {
                        // if wsu is not null, we know that T is TimestampedItem<?>
                        TimestampedItem<T> t1 = (TimestampedItem<T>) t;
                        return wsu.handleEvent(t1.item(), 0, t1.timestamp());
                    });
        }
        boolean bufferEmpty = emitFromTraverser(traverser);
        if (bufferEmpty) {
            traverser = null;
        }
        return bufferEmpty && buffer.isClosed();
    }

    @Override
    public void close() {
        if (initialized) {
            destroyFn.accept(src);
        }
    }
}
