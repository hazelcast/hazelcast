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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.impl.connector.ConvenientSourceP.SourceBufferConsumerSide;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import com.hazelcast.jet.pipeline.SourceBuilder.TimestampedSourceBuffer;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Queue;

import static com.hazelcast.jet.impl.JetEvent.jetEvent;

public class SourceBufferImpl<T> implements SourceBufferConsumerSide<T> {
    private final Queue<T> buffer = new ArrayDeque<>();
    private final Traverser<T> traverser = buffer::poll;
    private final boolean isBatch;
    private boolean isClosed;

    private SourceBufferImpl(boolean isBatch) {
        this.isBatch = isBatch;
    }

    final void addInternal(T item) {
        if (isClosed) {
            throw new IllegalStateException("Buffer is closed, can't add more items");
        }
        buffer.add(item);
    }

    public final int size() {
        return buffer.size();
    }

    @Override
    public final Traverser<T> traverse() {
        return traverser;
    }

    public final boolean isEmpty() {
        return buffer.isEmpty();
    }

    public final void close() {
        if (!isBatch) {
            throw new JetException("a streaming source must not close the buffer, only batch source can");
        }
        this.isClosed = true;
    }

    @Override
    public final boolean isClosed() {
        return isClosed;
    }

    public static class Plain<T> extends SourceBufferImpl<T> implements SourceBuffer<T> {
        public Plain(boolean isBatch) {
            super(isBatch);
        }

        @Override
        public void add(@Nonnull T item) {
            addInternal(item);
        }
    }

    public static class Timestamped<T>
            extends SourceBufferImpl<JetEvent<T>>
            implements TimestampedSourceBuffer<T> {

        public Timestamped() {
            super(false);
        }

        @Override
        public void add(@Nonnull T item, long timestamp) {
            addInternal(jetEvent(timestamp, item));
        }
    }
}
