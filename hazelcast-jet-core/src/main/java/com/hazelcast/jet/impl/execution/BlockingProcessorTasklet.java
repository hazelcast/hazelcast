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
import com.hazelcast.jet.impl.execution.OutboxBlockingImpl.JobFutureCompleted;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Tasklet that drives a non-cooperative processor.
 */
public class BlockingProcessorTasklet extends ProcessorTaskletBase {

    public BlockingProcessorTasklet(
            ProcCtx context, Processor processor, List<? extends InboundEdgeStream> instreams,
            List<? extends OutboundEdgeStream> outstreams, SnapshotContext ssContext,
            OutboundCollector ssCollector) {
        super(context, processor, instreams, outstreams, ssContext, ssCollector);
        Preconditions.checkFalse(processor.isCooperative(), "Processor is cooperative");
    }

    @Override
    public final boolean isCooperative() {
        return false;
    }

    @Override
    public void init(CompletableFuture<Void> jobFuture) {
        super.init(jobFuture);
        ((OutboxBlockingImpl) getOutbox()).initJobFuture(jobFuture);
    }

    @Override @Nonnull
    public ProgressState call() {
        try {
            return super.call();
        } catch (JobFutureCompleted e) {
            return ProgressState.DONE;
        }
    }

    @Override
    protected OutboxImpl createOutboxInt(OutboundCollector[] outstreams, boolean hasSnapshot,
                                         ProgressTracker progTracker, SerializationService serializationService) {
        return new OutboxBlockingImpl(outstreams, hasSnapshot, progTracker, serializationService);
    }
}
