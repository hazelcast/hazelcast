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

import static com.hazelcast.jet2.impl.TaskletResult.DONE;
import static com.hazelcast.jet2.impl.TaskletResult.MADE_PROGRESS;
import static com.hazelcast.jet2.impl.TaskletResult.NO_PROGRESS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;

public class ProcessorTasklet implements Tasklet {

    private final Processor processor;
    private final Queue<ArrayList<InboundEdgeStream>> instreamGroupQueue;
    private CircularCursor<InboundEdgeStream> instreamCursor;
    private final ArrayDequeWithPredicate inbox;
    private final ArrayDequeOutbox outbox;
    private final OutboundEdgeStream[] outStreams;
    private final ProgressTracker progTracker = new ProgressTracker();

    private InboundEdgeStream currInstream;
    private boolean currInstreamExhausted;
    private boolean processorCompleted;

    public ProcessorTasklet(
            Processor processor, List<InboundEdgeStream> instreams, List<OutboundEdgeStream> outstreams
    ) {
        Preconditions.checkNotNull(processor, "processor");
        this.processor = processor;
        this.instreamGroupQueue = instreams
                .stream()
                .collect(groupingBy(InboundEdgeStream::priority, TreeMap::new, toCollection(ArrayList::new)))
                .entrySet().stream()
                .map(Entry::getValue).collect(toCollection(ArrayDeque::new));
        this.inbox = new ArrayDequeWithPredicate();
        this.outbox = new ArrayDequeOutbox(outstreams.size());
        this.outStreams = outstreams.toArray(new OutboundEdgeStream[instreams.size()]);
        this.instreamCursor = popInboundStreamGroup();
    }

    @Override
    public TaskletResult call() throws Exception {
        progTracker.reset();
        tryFillInbox();
        if (progTracker.isDone()) {
            completeIfNeeded();
        } else if (!inbox.isEmpty()) {
            tryProcess();
        } else if (currInstreamExhausted) {
            progTracker.update(MADE_PROGRESS);
            if (processor.complete(currInstream.ordinal())) {
                currInstream = null;
            }
        }
        tryEmptyOutbox();
        return TaskletResult.valueOf(progTracker);
    }

    private CircularCursor<InboundEdgeStream> popInboundStreamGroup() {
        return Optional.ofNullable(instreamGroupQueue.poll()).map(CircularCursor::new).orElse(null);
    }

    private void tryFillInbox() {
        // we have more items to process, or current inbound stream is done but not yet completed
        if (!inbox.isEmpty() || currInstream != null && currInstreamExhausted) {
            progTracker.update(NO_PROGRESS);
            return;
        }
        if (instreamCursor == null) {
            return;
        }
        final InboundEdgeStream first = instreamCursor.value();
        TaskletResult result;
        do {
            currInstream = instreamCursor.value();
            result = currInstream.drainAvailableItemsInto(inbox);
            currInstreamExhausted = result.isDone();
            progTracker.update(result);
            if (currInstreamExhausted) {
                instreamCursor.remove();
            }
            if (!instreamCursor.advance()) {
                instreamCursor = popInboundStreamGroup();
                break;
            }
        } while (!result.isMadeProgress() && instreamCursor.value() != first);
        progTracker.notDone();
    }

    private void tryProcess() {
        final int inboundOrdinal = currInstream.ordinal();
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

    private void tryEmptyOutbox() {
        nextOutboundStream:
        for (int i = 0; i < outbox.queueCount(); i++) {
            final Queue q = outbox.queueWithOrdinal(i);
            for (Object item; (item = q.peek()) != null; ) {
                if (!outStreams[i].offer(item)) {
                    progTracker.notDone();
                    continue nextOutboundStream;
                }
                q.remove();
                progTracker.update(MADE_PROGRESS);
            }
        }
    }
}

