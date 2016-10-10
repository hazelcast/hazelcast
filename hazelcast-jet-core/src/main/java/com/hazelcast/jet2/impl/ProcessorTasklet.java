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

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.RandomAccess;

import static com.hazelcast.jet2.impl.TaskletResult.DONE;
import static com.hazelcast.jet2.impl.TaskletResult.MADE_PROGRESS;
import static com.hazelcast.jet2.impl.TaskletResult.NO_PROGRESS;
import static java.util.Map.Entry.comparingByKey;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

public class ProcessorTasklet implements Tasklet {

    private final Processor processor;
    private final List<ArrayList<InboundEdgeStream>> prioritizedInboundStreams;
    private CircularCursor<InboundEdgeStream> inboundStreamCursor;
    private final ArrayDequeWithObserver inbox;
    private final ArrayDequeOutbox outbox;
    private final OutboundEdgeStream[] outboundStreams;
    private final ProgressTracker progTracker = new ProgressTracker();

    private InboundEdgeStream selectedInboundStream;
    private boolean processorCompleted;

    public ProcessorTasklet(
            Processor processor, List<InboundEdgeStream> inboundStreams, List<OutboundEdgeStream> outboundStreams
    ) {
        Preconditions.checkNotNull(processor, "processor");
        this.processor = processor;
        this.prioritizedInboundStreams = inboundStreams
                .stream()
                .collect(groupingBy(InboundEdgeStream::priority, toCollection(ArrayList::new)))
                .entrySet().stream()
                .sorted(comparingByKey())
                .map(Entry::getValue).collect(toList());
        this.inbox = new ArrayDequeWithObserver();
        this.outbox = new ArrayDequeOutbox(outboundStreams.size());
        this.outboundStreams = outboundStreams.toArray(new OutboundEdgeStream[inboundStreams.size()]);
        popInboundStreamGroup();
    }

    @Override
    public TaskletResult call() throws Exception {
        progTracker.reset();
        tryFillInbox();
        if (progTracker.isDone()) {
            completeIfNeeded();
        } else if (!inbox.isEmpty()) {
            tryProcess();
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
            progTracker.update(NO_PROGRESS);
            return;
        }
        if (inboundStreamCursor == null) {
            return;
        }
        final InboundEdgeStream first = inboundStreamCursor.value();
        TaskletResult result;
        do {
            selectedInboundStream = inboundStreamCursor.value();
            result = selectedInboundStream.drainAvailableItemsInto(inbox);
            progTracker.update(result);
            if (result.isDone()) {
                inboundStreamCursor.remove();
            }
            if (!inboundStreamCursor.advance()) {
                popInboundStreamGroup();
                break;
            }
        } while (!result.isMadeProgress() && inboundStreamCursor.value() != first);
        progTracker.notDone();
    }

    private void tryProcess() {
        final int inboundOrdinal = selectedInboundStream.ordinal();
        for (Object item; (item = inbox.peek()) != null && !outbox.isHighWater();) {
            progTracker.update(MADE_PROGRESS);
            if (!processor.process(inboundOrdinal, item)) {
                break;
            }
            inbox.remove();
        }
    }

    private void popInboundStreamGroup() {
        this.inboundStreamCursor = !prioritizedInboundStreams.isEmpty()
                ? new CircularCursor<>(prioritizedInboundStreams.remove(0)) : null;
    }

    private void completeIfNeeded() {
        if (processorCompleted) {
            return;
        }
        if (processor.complete()) {
            progTracker.update(DONE);
            processorCompleted = true;
        } else {
            progTracker.update(MADE_PROGRESS);
        }
    }

    private void offerPendingOutput() {
        nextOutboundStream:
        for (int i = 0; i < outbox.queueCount(); i++) {
            final Queue q = outbox.queueWithOrdinal(i);
            for (Object item; (item = q.peek()) != null; ) {
                if (!outboundStreams[i].offer(item)) {
                    progTracker.notDone();
                    continue nextOutboundStream;
                }
                q.remove();
                progTracker.update(MADE_PROGRESS);
            }
        }
    }
}

