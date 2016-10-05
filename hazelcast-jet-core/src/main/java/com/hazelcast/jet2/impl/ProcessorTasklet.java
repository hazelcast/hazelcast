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

import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProgressTracker;
import com.hazelcast.util.Preconditions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;

import static com.hazelcast.jet2.impl.TaskletResult.MADE_PROGRESS;
import static java.util.Map.Entry.comparingByKey;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

public class ProcessorTasklet implements Tasklet {

    private final int bufferSize = 16;

    private final Processor processor;
    private final List<ArrayList<QueueHead>> prioritizedQueueHeads;
    private CircularCursor<QueueHead> queueHeadCursor;
    private final Queue pendingInput;
    private final ArrayDequeCollector pendingOutput;
    private final QueueTail[] queueTails;
    private final ProgressTracker progTracker = new ProgressTracker();

    private QueueHead currentQueueHead;
    private boolean processorCompleted;

    public ProcessorTasklet(Processor processor, List<QueueHead> queueHeads, List<QueueTail> queueTails) {
        Preconditions.checkNotNull(processor, "processor");
        Preconditions.checkFalse(queueHeads.isEmpty(), "There must be at least one queue head");
        Preconditions.checkFalse(queueTails.isEmpty(), "There must be at least one queue tail");
        this.processor = processor;
        this.prioritizedQueueHeads = queueHeads.stream()
                                               .collect(groupingBy(QueueHead::priority, toCollection(ArrayList::new)))
                                               .entrySet().stream()
                                               .sorted(comparingByKey())
                                               .map(Entry::getValue).collect(toList());
        this.pendingInput = new ArrayDeque();
        this.pendingOutput = new ArrayDequeCollector(queueTails.size());
        this.queueTails = queueTails.toArray(new QueueTail[queueHeads.size()]);
        popQueueHeadGroup();
    }

    @Override
    public TaskletResult call() throws Exception {
        progTracker.reset();
        ensurePendingInput();
        if (progTracker.isDone()) {
            idempotentComplete();
        } else if (!pendingInput.isEmpty()) {
            final int inputOrdinal = currentQueueHead.ordinal();
            final Queue outQ = pendingOutput.queueWithOrdinal(inputOrdinal);
            for (Object item; (item = pendingInput.peek()) != null && outQ.size() < bufferSize;) {
                progTracker.update(MADE_PROGRESS);
                if (!processor.process(inputOrdinal, item)) {
                    break;
                }
                pendingInput.remove();
            }
        } else if (currentQueueHead.isDone()) {
            progTracker.update(MADE_PROGRESS);
            if (processor.complete(currentQueueHead.ordinal())) {
                currentQueueHead = null;
            }
        }
        offerPendingOutput();
        return TaskletResult.valueOf(progTracker);
    }

    private void ensurePendingInput() {
        if (!pendingInput.isEmpty() || currentQueueHead != null && currentQueueHead.isDone()) {
            progTracker.notDone();
            return;
        }
        if (queueHeadCursor == null) {
            return;
        }
        final QueueHead first = queueHeadCursor.value();
        TaskletResult result;
        do {
            currentQueueHead = queueHeadCursor.value();
            result = currentQueueHead.drainTo(pendingInput);
            progTracker.update(result);
            if (result.isDone()) {
                queueHeadCursor.remove();
            }
            if (!queueHeadCursor.advance()) {
                popQueueHeadGroup();
                break;
            }
        } while (!result.isMadeProgress() && queueHeadCursor.value() != first);
        progTracker.notDone();
    }

    private void popQueueHeadGroup() {
        this.queueHeadCursor = !prioritizedQueueHeads.isEmpty()
                ? new CircularCursor<>(prioritizedQueueHeads.remove(0)) : null;
    }

    private boolean idempotentComplete() {
        if (processorCompleted) {
            return true;
        }
        if (processor.complete()) {
            processorCompleted = true;
        } else {
            progTracker.notDone();
        }
        return processorCompleted;
    }

    private void offerPendingOutput() {
        for (int i = 0; i < pendingOutput.queueCount(); i++) {
            final Queue q = pendingOutput.queueWithOrdinal(i);
            for (Object item; (item = q.peek()) != null; ) {
                if (!queueTails[i].offer(item)) {
                    progTracker.notDone();
                    return;
                }
                progTracker.update(MADE_PROGRESS);
            }
        }
    }
}

