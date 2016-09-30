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
import java.util.Map;

import static com.hazelcast.jet2.impl.TaskletResult.MADE_PROGRESS;
import static com.hazelcast.jet2.impl.TaskletResult.NO_PROGRESS;

public class ConsumerTasklet<I> implements Tasklet {

    private final Consumer<? super I> consumer;
    private final RemovableCircularCursor<QueueHead<? extends I>> queueHeadCursor;
    private Cursor<? extends I> chunkCursor;

    public ConsumerTasklet(Consumer<? super I> consumer, Map<String, QueueHead<? extends I>> queueHeads) {
        Preconditions.checkNotNull(consumer, "consumer");
        Preconditions.checkTrue(!queueHeads.isEmpty(), "There must be at least one input");

        this.consumer = consumer;
        this.queueHeadCursor = new RemovableCircularCursor<>(new ArrayList<>(queueHeads.values()));
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
        Chunk<? extends I> chunk = pollChunk();
        if (chunk == null) {
            consumer.complete();
            return TaskletResult.DONE;
        }
        if (chunk.isEmpty()) {
            return didPendingWork ? MADE_PROGRESS : NO_PROGRESS;
        }
        chunkCursor = chunk.cursor();
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

    private Chunk<? extends I> pollChunk() {
        QueueHead<? extends I> end = queueHeadCursor.value();
        Chunk<? extends I> result = null;
        while (queueHeadCursor.advance()) {
            QueueHead<? extends I> current = queueHeadCursor.value();
            Chunk<? extends I> chunk = current.pollChunk();
            if (chunk == null) {
                queueHeadCursor.remove();
            } else {
                result = chunk;
                if (!chunk.isEmpty()) {
                    break;
                }
            }
            if (current == end) {
                break;
            }
        }
        return result;
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
