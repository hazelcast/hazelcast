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

public class ProcessorTasklet<I, O> implements Tasklet {

    private final Processor<? super I, ? extends O> processor;
    private final ArrayListCollector<O> collector;
    private final Map<String, Input<? extends I>> inputs;
    private final RemovableCircularCursor<Map.Entry<String, Input<? extends I>>> inputCursor;
    private final Cursor<Output<O>> outputCursor;

    private Cursor<O> collectorCursor;
    private Cursor<? extends I> chunkCursor;
    private boolean processingComplete;


    public ProcessorTasklet(Processor<? super I, ? extends O> processor,
                            Map<String, Input<? extends I>> inputs,
                            Map<String, Output<O>> outputs) {
        Preconditions.checkNotNull(processor, "processor");
        Preconditions.checkTrue(!inputs.isEmpty(), "There must be at least one input");
        Preconditions.checkTrue(!outputs.isEmpty(), "There must be at least one output");

        this.processor = processor;
        this.inputs = inputs;
        this.outputCursor = new ListCursor<>(new ArrayList<>(outputs.values()));
        this.collector = new ArrayListCollector<>();
        this.inputCursor = new RemovableCircularCursor<>(new ArrayList<>(this.inputs.entrySet()));

    }

    @Override
    public TaskletResult call() throws Exception {

        boolean didPendingWork = false;
        // process pending output
        if (collectorCursor != null) {
            switch (tryOffer()) {
                case OFFERED_NONE:
                    return TaskletResult.NO_PROGRESS;
                case OFFERED_SOME:
                    return TaskletResult.MADE_PROGRESS;
                case OFFERED_ALL:
                    if (processingComplete) {
                        return TaskletResult.DONE;
                    }
                    didPendingWork = true;
                    break;
            }
        }

        // process pending input
        if (chunkCursor != null) {
            boolean processedAll = tryProcess();
            if (!collector.isEmpty()) {
                switch (tryOffer()) {
                    case OFFERED_ALL:
                        if (!processedAll) {
                            return TaskletResult.MADE_PROGRESS;
                        }
                        didPendingWork = true;
                        break;
                    case OFFERED_SOME:
                    case OFFERED_NONE:
                        return TaskletResult.MADE_PROGRESS;
                }
            }
        }
        Chunk<? extends I> chunk = getNextChunk();

        if (chunk == null) {
            if (inputs.size() > 0) {
                // did not find anything to read, but inputs are not complete yet
                return didPendingWork ? TaskletResult.MADE_PROGRESS : TaskletResult.NO_PROGRESS;
            } else {
                // done reading inputs, try complete the processing
                processingComplete = tryComplete();
                if (processingComplete && collector.isEmpty()) {
                    return TaskletResult.DONE;
                }

                collectorCursor = collector.cursor();
                collectorCursor.advance();
                OfferResult result = tryOffer();
                if (processingComplete && result == OfferResult.OFFERED_ALL) {
                    return TaskletResult.DONE;
                }
                return TaskletResult.MADE_PROGRESS;
            }
        }

        chunkCursor = chunk.cursor();
        chunkCursor.advance();
        tryProcess();
        if (!collector.isEmpty()) {
            collectorCursor = collector.cursor();
            collectorCursor.advance();
            tryOffer();
        }
        // made progress no matter what, as we read a new input chunk
        return TaskletResult.MADE_PROGRESS;
    }

    private boolean tryProcess() {
        do {
            boolean processed = processor.process(inputCursor.value().getKey(), chunkCursor.value(), collector);
            if (!processed) {
                return false;
            }
        } while ((chunkCursor.advance()));

        chunkCursor = null;
        return true;
    }

    private boolean tryComplete() {
        collector.clear();
        return processor.complete(collector);
    }

    private Chunk<? extends I> getNextChunk() {
        Input<? extends I> end = inputCursor.value().getValue();
        while (inputCursor.advance()) {
            Input<? extends I> current = inputCursor.value().getValue();
            Chunk<? extends I> chunk = current.nextChunk();
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

    private OfferResult tryOffer() {
        boolean pushedSome = false;
        do {
            do {
                boolean pushed = outputCursor.value().offer(collectorCursor.value());
                pushedSome |= pushed;
                if (!pushed) {
                    return pushedSome ? OfferResult.OFFERED_SOME : OfferResult.OFFERED_NONE;
                }
            } while (outputCursor.advance());
            outputCursor.reset();
            outputCursor.advance();
        } while (collectorCursor.advance());

        collector.clear();
        collectorCursor = null;
        return OfferResult.OFFERED_ALL;
    }

    private enum OfferResult {
        OFFERED_ALL,
        OFFERED_SOME,
        OFFERED_NONE,
    }
}

