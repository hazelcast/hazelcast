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
import java.util.Optional;
import java.util.Queue;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.hazelcast.jet2.impl.TaskletResult.DONE;
import static com.hazelcast.jet2.impl.TaskletResult.MADE_PROGRESS;
import static com.hazelcast.jet2.impl.TaskletResult.NO_PROGRESS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;

public class ProcessorTasklet implements Tasklet {

    private final Processor processor;
    private final Queue<ArrayList<InboundEdgeStream>> inboundStreamsQueue;
    private CircularCursor<InboundEdgeStream> inboundStreamCursor;
    private final ArrayDequeWithObserver inbox;
    private final ArrayDequeOutbox outbox;
    private final OutboundEdgeStream[] outboundStreams;
    private final ProgressTracker progTracker = new ProgressTracker();

    private InboundEdgeStream currentInboundStream;
    private boolean currentInboundStreamDone;
    private boolean processorCompleted;

    public ProcessorTasklet(
            Processor processor, List<InboundEdgeStream> inboundStreams, List<OutboundEdgeStream> outboundStreams
    ) {
        Preconditions.checkNotNull(processor, "processor");
        this.processor = processor;
        this.inboundStreamsQueue = inboundStreams
                .stream()
                .collect(groupingBy(InboundEdgeStream::priority, TreeMap::new, toCollection(ArrayList::new)))
                .entrySet().stream()
                .map(Entry::getValue).collect(Collectors.toCollection(ArrayDeque::new));
        this.inbox = new ArrayDequeWithObserver();
        this.outbox = new ArrayDequeOutbox(outboundStreams.size());
        this.outboundStreams = outboundStreams.toArray(new OutboundEdgeStream[inboundStreams.size()]);
        this.inboundStreamCursor = popInboundStreamGroup();
    }

    @Override
    public TaskletResult call() throws Exception {
        progTracker.reset();
        tryFillInbox();
        if (progTracker.isDone()) {
            completeIfNeeded();
        } else if (!inbox.isEmpty()) {
            tryProcess();
        } else if (currentInboundStreamDone) {
            progTracker.update(MADE_PROGRESS);
            if (processor.complete(currentInboundStream.ordinal())) {
                currentInboundStream = null;
            }
        }
        trySendOutbox();
        return TaskletResult.valueOf(progTracker);
    }

    private CircularCursor<InboundEdgeStream> popInboundStreamGroup() {
        return Optional.ofNullable(inboundStreamsQueue.poll()).map(CircularCursor::new).orElse(null);
    }

    private void tryFillInbox() {
        // we have more items to process, or current inbound stream is done but not yet completed
        if (!inbox.isEmpty() || currentInboundStream != null && currentInboundStreamDone) {
            progTracker.update(NO_PROGRESS);
            return;
        }
        if (inboundStreamCursor == null) {
            return;
        }
        final InboundEdgeStream first = inboundStreamCursor.value();
        TaskletResult result;
        do {
            currentInboundStream = inboundStreamCursor.value();
            result = currentInboundStream.drainAvailableItemsInto(inbox);
            currentInboundStreamDone = result.isDone();
            progTracker.update(result);
            if (currentInboundStreamDone) {
                inboundStreamCursor.remove();
            }
            if (!inboundStreamCursor.advance()) {
                inboundStreamCursor = popInboundStreamGroup();
                break;
            }
        } while (!result.isMadeProgress() && inboundStreamCursor.value() != first);
        progTracker.notDone();
    }

    private void tryProcess() {
        final int inboundOrdinal = currentInboundStream.ordinal();
        for (Object item; (item = inbox.peek()) != null && !outbox.isHighWater(); ) {
            progTracker.update(MADE_PROGRESS);
            if (!processor.process(inboundOrdinal, item)) {
                break;
            }
            inbox.remove();
        }
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

    private void trySendOutbox() {
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

