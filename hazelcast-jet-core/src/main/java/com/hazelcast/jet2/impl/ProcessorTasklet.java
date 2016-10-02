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

import com.hazelcast.jet2.Cursor;
import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProgressTracker;
import com.hazelcast.util.Preconditions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Map;

import static com.hazelcast.jet2.impl.TaskletResult.MADE_PROGRESS;

public class ProcessorTasklet<I, O> implements Tasklet {

    private final int bufferSize = 16;

    private final Processor<? super I, ? extends O> processor;
    private final Deque<I> pendingInput;
    private final ArrayDequeCollector<O> pendingOutput;
    private RemovableCircularCursor<Map.Entry<String, QueueHead<? extends I>>> queueHeadCursor;
    private final Cursor<QueueTail<? super O>> queueTailCursor;
    private final ProgressTracker progTracker = new ProgressTracker();

    private boolean processorCompleted;

    public ProcessorTasklet(Processor<? super I, ? extends O> processor,
                            Map<String, QueueHead<? extends I>> queueHeads,
                            Map<String, QueueTail<? super O>> queueTails) {
        Preconditions.checkNotNull(processor, "processor");
        Preconditions.checkFalse(queueHeads.isEmpty(), "There must be at least one input");
        Preconditions.checkFalse(queueTails.isEmpty(), "There must be at least one output");

        this.processor = processor;
        if (!queueHeads.isEmpty()) {
            this.queueHeadCursor = new RemovableCircularCursor<>(new ArrayList<>(queueHeads.entrySet()));
        }
        this.queueTailCursor = queueTails.isEmpty() ? null : new ListCursor<>(new ArrayList<>(queueTails.values()));
        this.pendingInput = new ArrayDeque<>();
        this.pendingOutput = new ArrayDequeCollector<>();
    }

    @Override
    public TaskletResult call() throws Exception {
        progTracker.reset();
        ensurePendingInput();
        if (progTracker.isDone()) {
            if (!processorCompleted) {
                processorCompleted = processor.complete(pendingOutput);
            }
        } else if (!pendingInput.isEmpty()) {
            progTracker.update(MADE_PROGRESS);
            while (processor.process(queueHeadCursor.value().getKey(), pendingInput.peek(), pendingOutput)) {
                pendingInput.remove();
            }
        }
        offerPendingOutput();
        return TaskletResult.valueOf(progTracker);
    }

    private void ensurePendingInput() {
        if (queueHeadCursor == null) {
            return;
        }
        if (pendingInput.size() >= bufferSize / 2) {
            progTracker.notDone();
            return;
        }
        final QueueHead<? extends I> first = queueHeadCursor.value().getValue();
        QueueHead<? extends I> current = first;
        do {
            final TaskletResult result = current.drainTo(pendingInput);
            progTracker.update(result);
            if (result.isDone()) {
                queueHeadCursor.remove();
            }
            if (!queueHeadCursor.advance()) {
                return;
            }
            current = queueHeadCursor.value().getValue();
        } while (current != first && pendingInput.size() < bufferSize);
        progTracker.notDone();
    }

    private void offerPendingOutput() {
        for (O item; (item = pendingOutput.peek()) != null;) {
            if (!queueTailCursor.value().offer(item)) {
                progTracker.notDone();
                return;
            }
            progTracker.update(MADE_PROGRESS);
            pendingOutput.remove();
        }
    }
}

