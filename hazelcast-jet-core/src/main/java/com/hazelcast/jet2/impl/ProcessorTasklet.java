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
import java.util.Iterator;
import java.util.Map;

public class ProcessorTasklet implements Tasklet {

    private final Processor processor;
    private final ArrayListCollector collector;
    private final Map<String, Input> inputs;

    private Cursor<Object> collectorCursor;
    private Iterator<Map.Entry<String, Input>> inputIterator;
    private final Cursor<Output> outputCursor;
    private Map.Entry<String, Input> currentInput;


    public ProcessorTasklet(Processor<?, ?> processor,
                            Map<String, Input> inputs,
                            Map<String, Output> outputs) {
        Preconditions.checkNotNull(processor, "processor");
        Preconditions.checkTrue(!inputs.isEmpty(), "There must be at least one input");
        Preconditions.checkTrue(!outputs.isEmpty(), "There must be at least one output");

        this.processor = processor;
        this.inputs = inputs;
        this.outputCursor = new ListCursor<>(new ArrayList<>(outputs.values()));
        this.collector = new ArrayListCollector();

    }

    @Override
    public TaskletResult call() throws Exception {
        // if there is something already in output buffer that needs processing
        if (collectorCursor != null) {
            switch (tryPush()) {
                case PUSHED_NONE:
                    return TaskletResult.NO_PROGRESS;
                case PUSHED_SOME:
                    return TaskletResult.MADE_PROGRESS;
                case PUSHED_ALL:
                    break;
            }
        }

        Chunk chunk = getNextChunk();
        // no more chunks possible
        if (chunk == null) {
            if (inputs.size() == 0) {
                PushResult result = complete();
                switch (result) {
                    case PUSHED_ALL:
                        return TaskletResult.DONE;
                    case PUSHED_SOME:
                        return TaskletResult.MADE_PROGRESS;
                    case PUSHED_NONE:
                        return TaskletResult.NO_PROGRESS;
                }
                if (result == PushResult.PUSHED_ALL) {
                    return TaskletResult.DONE;
                } else {
                    return TaskletResult.MADE_PROGRESS;
                }
            }
            return TaskletResult.MADE_PROGRESS;
        }

        processChunk(chunk);
        return TaskletResult.MADE_PROGRESS;
    }

    private PushResult processChunk(Chunk chunk) {
        Cursor chunkCursor = chunk.cursor();
        while (chunkCursor.advance()) {
            processor.process(currentInput.getKey(), chunkCursor.value(), collector);
        }

        if (collector.isEmpty()) {
            return PushResult.PUSHED_ALL;
        }
        collectorCursor = collector.cursor();
        collectorCursor.advance();

        return tryPush();
    }

    private PushResult complete() {
        processor.complete(collector);
        if (collector.isEmpty()) {
            return PushResult.PUSHED_ALL;
        }
        return tryPush();
    }

    private Chunk getNextChunk() {
        inputIterator = inputs.entrySet().iterator();
        while (inputIterator.hasNext()) {
            currentInput = inputIterator.next();
            Chunk chunk = currentInput.getValue().nextChunk();
            if (chunk == null) {
                inputIterator.remove();
            } else if (!chunk.isEmpty()) {
                return chunk;
            }
        }
        return null;
    }

    private PushResult tryPush() {
        boolean pushedSome = false;
        do {
            do {
                boolean pushed = outputCursor.value().collect(collectorCursor.value());
                pushedSome |= pushed;
                if (!pushed) {
                    return pushedSome ? PushResult.PUSHED_SOME : PushResult.PUSHED_NONE;
                }
            } while (outputCursor.advance());
        } while (collectorCursor.advance());

        outputCursor.reset();
        outputCursor.advance();
        collectorCursor = null;
        collector.clear();
        return PushResult.PUSHED_ALL;
    }

    private enum PushResult {
        PUSHED_ALL,
        PUSHED_SOME,
        PUSHED_NONE,
    }
}

