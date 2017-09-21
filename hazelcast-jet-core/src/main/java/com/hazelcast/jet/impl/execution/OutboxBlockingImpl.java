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

import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.util.concurrent.IdleStrategy;

import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class OutboxBlockingImpl extends OutboxImpl {

    private static final IdleStrategy IDLER =
            new BackoffIdleStrategy(0, 0, MICROSECONDS.toNanos(1), MILLISECONDS.toNanos(1));

    private CompletableFuture<Void> jobFuture;

    public OutboxBlockingImpl(OutboundCollector[] outstreams, boolean hasSnapshot,
                              ProgressTracker progTracker, SerializationService serializationService) {
        super(outstreams, hasSnapshot, progTracker, serializationService);
    }

    @Override
    protected ProgressState doOffer(OutboundCollector collector, Object item) {
        for (long idleCount = 0; ;) {
            ProgressState result = super.doOffer(collector, item);
            if (result.isDone()) {
                return result;
            }
            if (jobFuture.isDone()) {
                throw new JobFutureCompleted();
            }
            if (result.isMadeProgress()) {
                idleCount = 0;
            } else {
                IDLER.idle(++idleCount);
            }
        }
    }

    public void initJobFuture(CompletableFuture<Void> jobFuture) {
        this.jobFuture = jobFuture;
    }

    public static class JobFutureCompleted extends RuntimeException {
    }
}
