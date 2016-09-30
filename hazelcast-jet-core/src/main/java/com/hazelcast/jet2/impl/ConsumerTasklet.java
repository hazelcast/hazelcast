/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2.impl;

import com.hazelcast.jet2.Chunk;
import com.hazelcast.jet2.Consumer;
import com.hazelcast.jet2.Cursor;
import com.hazelcast.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConsumerTasklet<T> implements Tasklet {

    private final List<QueueHead<? extends T>> queueHeads;
    private final Consumer<? super T> consumer;
    private final RemovableCircularCursor<QueueHead<? extends T>> inputCursor;
    private Cursor<? extends T> chunkCursor;

    public ConsumerTasklet(Consumer<? super T> consumer, Map<String, QueueHead<? extends T>> inputs) {
        Preconditions.checkNotNull(consumer, "consumer");
        Preconditions.checkTrue(!inputs.isEmpty(), "There must be at least one input");

        this.consumer = consumer;
        this.queueHeads = new ArrayList<>(inputs.values());
        this.inputCursor = new RemovableCircularCursor<>(this.queueHeads);
        inputCursor.advance();
    }

    @Override
    public TaskletResult call() {
        boolean didPendingWork = false;
        if (chunkCursor != null) {
            // retry to consume the last chunk
            ConsumeResult result = tryConsume();
            switch (result) {
                case CONSUMED_ALL:
                    didPendingWork = true;
                    // move on to next chunk
                    break;
                case CONSUMED_SOME:
                    return TaskletResult.MADE_PROGRESS;
                case CONSUMED_NONE:
                    return TaskletResult.NO_PROGRESS;
            }
        }
        Chunk<? extends T> chunk = getNextChunk();
        if (chunk == null) {
            if (queueHeads.isEmpty()) {
                consumer.complete();
                return TaskletResult.DONE;
            }
            // could not find any chunk to read
            return didPendingWork ? TaskletResult.MADE_PROGRESS : TaskletResult.NO_PROGRESS;
        }

        chunkCursor = chunk.cursor();
        chunkCursor.advance();
        tryConsume();
        return TaskletResult.MADE_PROGRESS;
    }

    private ConsumeResult tryConsume() {
        boolean consumedSome = false;
        do {
            chunkCursor.value();
            boolean consumed = consumer.consume(chunkCursor.value());
            consumedSome |= consumed;
            if (!consumed) {
                return consumedSome ? ConsumeResult.CONSUMED_SOME : ConsumeResult.CONSUMED_NONE;
            }
        } while (chunkCursor.advance());
        chunkCursor = null;
        return ConsumeResult.CONSUMED_ALL;
    }

    private Chunk<? extends T> getNextChunk() {
        QueueHead<? extends T> end = inputCursor.value();
        while (inputCursor.advance()) {
            QueueHead<? extends T> current = inputCursor.value();
            Chunk<? extends T> chunk = current.pollChunk();
            if (chunk == null) {
                inputCursor.remove();
                if (current == end) {
                    break;
                }
                continue;
            }
            if (!chunk.isEmpty()) {
                return chunk;
            }
            if (current == end) {
                break;
            }
        }
        return null;
    }

    @Override
    public boolean isBlocking() {
        return consumer.isBlocking();
    }

    private enum ConsumeResult {
        CONSUMED_NONE,
        CONSUMED_SOME,
        CONSUMED_ALL
    }
}
