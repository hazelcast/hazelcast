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

import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.execution.ExecutionService.IDLER;
import static com.hazelcast.jet.impl.util.DoneItem.DONE_ITEM;

/**
 * Tasklet that drives a non-cooperative processor.
 */
public class BlockingProcessorTasklet extends ProcessorTaskletBase {

    private final BlockingOutbox outbox;
    private CompletableFuture<?> jobFuture;

    public BlockingProcessorTasklet(
            ProcCtx context, Processor processor, List<InboundEdgeStream> instreams,
            List<OutboundEdgeStream> outstreams
    ) {
        super(context, processor, instreams, outstreams);
        Preconditions.checkFalse(processor.isCooperative(), "Processor is cooperative");
        outbox = new BlockingOutbox();
    }

    @Override
    public final boolean isCooperative() {
        return false;
    }

    @Override
    public void init(CompletableFuture<Void> jobFuture) {
        super.init(jobFuture);
        this.jobFuture = jobFuture;
        initProcessor(outbox, jobFuture);
    }

    @Override @Nonnull
    public ProgressState call() {
        try {
            progTracker.reset();
            tryFillInbox();
            if (progTracker.isDone()) {
                complete();
            } else if (!inbox().isEmpty()) {
                processor.process(currInstream.ordinal(), inbox());
            }
            return progTracker.toProgressState();
        } catch (JobFutureCompletedExceptionally e) {
            return ProgressState.DONE;
        }
    }

    private void complete() {
        if (processor.complete()) {
            outbox.add(DONE_ITEM);
        } else {
            progTracker.notDone();
        }
    }

    private class BlockingOutbox implements Outbox {

        @Override
        public int bucketCount() {
            return outstreams.length;
        }

        @Override
        public boolean offer(int ordinal, @Nonnull Object item) {
            progTracker.madeProgress();
            if (ordinal != -1) {
                submit(outstreams[ordinal], item);
            } else {
                for (OutboundEdgeStream outstream : outstreams) {
                    submit(outstream, item);
                }
            }
            return true;
        }

        @Override
        public boolean offer(int[] ordinals, @Nonnull Object item) {
            progTracker.madeProgress();
            for (int ord : ordinals) {
                submit(outstreams[ord], item);
            }
            return true;
        }

        void add(@Nonnull Object item) {
            boolean accepted = outbox.offer(item);
            assert accepted : "Blocking outbox refused an item: " + item;
        }

        private void submit(OutboundEdgeStream outstream, @Nonnull Object item) {
            OutboundCollector collector = outstream.getCollector();
            for (long idleCount = 0; ;) {
                ProgressState result = (item != DONE_ITEM) ? collector.offer(item) : collector.close();
                if (result.isDone()) {
                    return;
                }
                if (jobFuture.isCompletedExceptionally()) {
                    throw new JobFutureCompletedExceptionally();
                }
                if (result.isMadeProgress()) {
                    idleCount = 0;
                } else {
                    IDLER.idle(++idleCount);
                }
            }
        }
    }

    private static class JobFutureCompletedExceptionally extends RuntimeException {
    }
}
