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
import com.hazelcast.jet2.Cursor;
import com.hazelcast.jet2.Processor;
import com.hazelcast.util.Preconditions;

import java.util.ArrayList;
import java.util.Map;

import static com.hazelcast.jet2.impl.ProcessorTasklet.OfferResult.ACCEPTED_ALL;
import static com.hazelcast.jet2.impl.ProcessorTasklet.OfferResult.ACCEPTED_NONE;
import static com.hazelcast.jet2.impl.ProcessorTasklet.OfferResult.ACCEPTED_SOME;
import static com.hazelcast.jet2.impl.TaskletResult.DONE;
import static com.hazelcast.jet2.impl.TaskletResult.MADE_PROGRESS;
import static com.hazelcast.jet2.impl.TaskletResult.NO_PROGRESS;

public class ProcessorTasklet<I, O> implements Tasklet {

    private final Processor<? super I, ? extends O> processor;
    private final ArrayListCollector<O> pendingOutputCollector;
    private final Map<String, QueueHead<? extends I>> queueHeads;
    private final RemovableCircularCursor<Map.Entry<String, QueueHead<? extends I>>> queueHeadCursor;
    private final Cursor<QueueTail<? super O>> queueTailCursor;

    private Cursor<O> pendingOutputCursor;
    private Cursor<? extends I> pendingInputCursor;
    private boolean processingComplete;


    public ProcessorTasklet(Processor<? super I, ? extends O> processor,
                            Map<String, QueueHead<? extends I>> queueHeads,
                            Map<String, QueueTail<? super O>> queueTails) {
        Preconditions.checkNotNull(processor, "processor");
        Preconditions.checkFalse(queueHeads.isEmpty(), "There must be at least one input");
        Preconditions.checkFalse(queueTails.isEmpty(), "There must be at least one output");

        this.processor = processor;
        this.queueHeads = queueHeads;
        this.queueHeadCursor = new RemovableCircularCursor<>(new ArrayList<>(this.queueHeads.entrySet()));
        this.pendingOutputCollector = new ArrayListCollector<>();
        this.queueTailCursor = new ListCursor<>(new ArrayList<>(queueTails.values()));
    }

    @Override
    public TaskletResult call() throws Exception {
        boolean didPendingWork = false;
        if (pendingOutputCursor != null) {
            switch (offerPendingOutput()) {
                case ACCEPTED_NONE:
                    return NO_PROGRESS;
                case ACCEPTED_SOME:
                    return MADE_PROGRESS;
                case ACCEPTED_ALL:
                    if (processingComplete) {
                        return DONE;
                    }
                    didPendingWork = true;
                    break;
            }
        }
        // Invariant at this point: there is no pending output

        if (pendingInputCursor != null) {
            boolean pendingInputDone = tryProcessPendingInput();
            if (!pendingOutputCollector.isEmpty()) {
                pendingOutputCursor = pendingOutputCollector.cursor();
                switch (offerPendingOutput()) {
                    case ACCEPTED_ALL:
                        if (!pendingInputDone) {
                            return MADE_PROGRESS;
                        }
                        didPendingWork = true;
                        break;
                    case ACCEPTED_SOME:
                    case ACCEPTED_NONE:
                        return MADE_PROGRESS;
                }
            }
        }

        // Invariant at this point: there is neither pending input nor pending output

        Chunk<? extends I> chunk = pollChunk();
        if (chunk == null) {
            processingComplete = tryComplete();
            if (processingComplete && pendingOutputCollector.isEmpty()) {
                return DONE;
            }
            pendingOutputCursor = pendingOutputCollector.cursor();
            OfferResult offerResult = offerPendingOutput();
            if (processingComplete && offerResult == ACCEPTED_ALL) {
                return DONE;
            }
            return MADE_PROGRESS;
        }
        if (chunk.isEmpty()) {
            return didPendingWork ? MADE_PROGRESS : NO_PROGRESS;
        }

        // Invariant at this point: there is pending input and no pending output

        pendingInputCursor = chunk.cursor();
        tryProcessPendingInput();
        if (!pendingOutputCollector.isEmpty()) {
            pendingOutputCursor = pendingOutputCollector.cursor();
            offerPendingOutput();
        }

        return MADE_PROGRESS;
    }

    private boolean tryProcessPendingInput() {
        do {
            boolean currentItemDone =
                    processor.process(queueHeadCursor.value().getKey(), pendingInputCursor.value(), pendingOutputCollector);
            if (!currentItemDone) {
                return false;
            }
        } while (pendingInputCursor.advance());

        pendingInputCursor = null;
        return true;
    }

    private boolean tryComplete() {
        pendingOutputCollector.clear();
        return processor.complete(pendingOutputCollector);
    }

    private Chunk<? extends I> pollChunk() {
        QueueHead<? extends I> end = queueHeadCursor.value().getValue();
        Chunk<? extends I> result = null;
        while (queueHeadCursor.advance()) {
            QueueHead<? extends I> current = queueHeadCursor.value().getValue();
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

    private OfferResult offerPendingOutput() {
        boolean pushedSome = false;
        do {
            do {
                boolean pushed = queueTailCursor.value().offer(pendingOutputCursor.value());
                pushedSome |= pushed;
                if (!pushed) {
                    return pushedSome ? ACCEPTED_SOME : ACCEPTED_NONE;
                }
            } while (queueTailCursor.advance());
            queueTailCursor.reset();
        } while (pendingOutputCursor.advance());

        pendingOutputCollector.clear();
        pendingOutputCursor = null;
        return ACCEPTED_ALL;
    }

    enum OfferResult {
        ACCEPTED_ALL,
        ACCEPTED_SOME,
        ACCEPTED_NONE,
    }
}

