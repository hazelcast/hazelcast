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

import com.hazelcast.jet2.Outbox;
import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProgressTracker;
import com.hazelcast.util.Preconditions;

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

    private final Processor processor;
    private final List<ArrayList<InboundEdgeStream>> prioritizedQueueHeads;
    private CircularCursor<InboundEdgeStream> queueHeadCursor;
    private final ArrayDequeWithObserver inbox;
    private final Outbox outbox;
    private final OutboundEdgeStream[] outboundStreams;
    private final ProgressTracker progTracker = new ProgressTracker();

    private InboundEdgeStream selectedInboundStream;
    private boolean processorCompleted;

    public ProcessorTasklet(
            Processor processor, List<InboundEdgeStream> inboundStreams, List<OutboundEdgeStream> outboundStreams
    ) {
        Preconditions.checkNotNull(processor, "processor");
        Preconditions.checkFalse(inboundStreams.isEmpty(), "There must be at least one queue head");
        Preconditions.checkFalse(outboundStreams.isEmpty(), "There must be at least one queue tail");
        this.processor = processor;
        this.prioritizedQueueHeads = inboundStreams
                .stream()
                .collect(groupingBy(InboundEdgeStream::priority, toCollection(ArrayList::new)))
                .entrySet().stream()
                .sorted(comparingByKey())
                .map(Entry::getValue).collect(toList());
        this.inbox = new ArrayDequeWithObserver();
        this.outbox = new Outbox(outboundStreams.size());
        this.outboundStreams = outboundStreams.toArray(new OutboundEdgeStream[inboundStreams.size()]);
        popQueueHeadGroup();
    }

    @Override
    public TaskletResult call() throws Exception {
        progTracker.reset();
        tryFillInbox();
        if (progTracker.isDone()) {
            completeIfNeeded();
        } else if (!inbox.isEmpty()) {
            final int inboundOrdinal = selectedInboundStream.ordinal();
            for (Object item; (item = inbox.peek()) != null && !outbox.isHighWater();) {
                progTracker.update(MADE_PROGRESS);
                if (!processor.process(inboundOrdinal, item)) {
                    break;
                }
                inbox.remove();
            }
        } else if (selectedInboundStream.isDone()) {
            progTracker.update(MADE_PROGRESS);
            if (processor.complete(selectedInboundStream.ordinal())) {
                selectedInboundStream = null;
            }
        }
        offerPendingOutput();
        return TaskletResult.valueOf(progTracker);
    }

    private void tryFillInbox() {
        if (!inbox.isEmpty() || selectedInboundStream != null && selectedInboundStream.isDone()) {
            progTracker.notDone();
            return;
        }
        if (queueHeadCursor == null) {
            return;
        }
        final InboundEdgeStream first = queueHeadCursor.value();
        TaskletResult result;
        do {
            selectedInboundStream = queueHeadCursor.value();
            result = selectedInboundStream.drainTo(inbox);
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

    private boolean completeIfNeeded() {
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
        for (int i = 0; i < outbox.queueCount(); i++) {
            final Queue q = outbox.queueWithOrdinal(i);
            for (Object item; (item = q.peek()) != null; ) {
                if (!outboundStreams[i].offer(item)) {
                    progTracker.notDone();
                    return;
                }
                progTracker.update(MADE_PROGRESS);
            }
        }
    }
}

