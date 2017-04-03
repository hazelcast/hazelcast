/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.Processor;
import com.hazelcast.jet.impl.util.ArrayDequeOutbox;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.DoneItem.DONE_ITEM;

/**
 * Tasklet that drives a cooperative processor.
 */
public class CooperativeProcessorTasklet extends ProcessorTaskletBase {
    private final ArrayDequeOutbox outbox;
    private boolean processorCompleted;

    public CooperativeProcessorTasklet(String vertexName, Processor.Context context, Processor processor,
                                       List<InboundEdgeStream> instreams, List<OutboundEdgeStream> outstreams) {
        super(vertexName, context, processor, instreams, outstreams);
        Preconditions.checkTrue(processor.isCooperative(), "Processor is non-cooperative");
        int[] highWaterMarks = Stream.of(this.outstreams).mapToInt(OutboundEdgeStream::getHighWaterMark).toArray();
        this.outbox = new ArrayDequeOutbox(outstreams.size(), highWaterMarks);
    }

    @Override
    public final boolean isCooperative() {
        return true;
    }

    @Override
    public void init(CompletableFuture<?> jobFuture) {
        initProcessor(outbox);
    }

    @Override @Nonnull
    public ProgressState call() {
        progTracker.reset();
        tryFillInbox();
        if (progTracker.isDone()) {
            completeIfNeeded();
        } else if (!inbox().isEmpty()) {
            tryProcessInbox();
        }
        tryFlushOutbox();
        return progTracker.toProgressState();
    }

    private void tryProcessInbox() {
        if (outbox.isHighWater()) {
            progTracker.notDone();
            return;
        }
        progTracker.madeProgress(true);
        final int inboundOrdinal = currInstream.ordinal();
        processor.process(inboundOrdinal, inbox());
        if (!inbox().isEmpty()) {
            progTracker.notDone();
        }
    }

    private void completeIfNeeded() {
        if (processorCompleted) {
            return;
        }
        if (outbox.isHighWater()) {
            progTracker.notDone();
            return;
        }
        progTracker.madeProgress(true);
        if (!processor.complete()) {
            progTracker.notDone();
            return;
        }
        processorCompleted = true;
        for (OutboundEdgeStream outstream : outstreams) {
            outbox.add(outstream.ordinal(), DONE_ITEM);
        }
    }

    private void tryFlushOutbox() {
        nextOutstream:
        for (int i = 0; i < outbox.bucketCount(); i++) {
            final Queue q = outbox.queueWithOrdinal(i);
            for (Object item; (item = q.peek()) != null; ) {
                OutboundCollector collector = outstreams[i].getCollector();
                final ProgressState state = (item != DONE_ITEM) ? collector.offer(item) : collector.close();
                progTracker.madeProgress(state.isMadeProgress());
                if (!state.isDone()) {
                    progTracker.notDone();
                    continue nextOutstream;
                }
                q.remove();
            }
        }
    }
}

