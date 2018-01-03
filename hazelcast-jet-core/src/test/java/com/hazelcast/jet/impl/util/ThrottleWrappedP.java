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

package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * A wrapper processor to throttle the output of a processor.
 */
public final class ThrottleWrappedP implements Processor {

    private final Processor wrappedProcessor;
    private final long itemsPerSecond;

    public ThrottleWrappedP(Processor wrappedProcessor, long itemsPerSecond) {
        checkNotNull(wrappedProcessor, "wrappedProcessor");
        checkTrue(wrappedProcessor.isCooperative(), "wrappedProcessor must be cooperative");

        this.itemsPerSecond = itemsPerSecond;
        this.wrappedProcessor = wrappedProcessor;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        wrappedProcessor.init(new ThrottlingOutbox(outbox), context);
    }

    @Override
    public boolean isCooperative() {
        return wrappedProcessor.isCooperative();
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        wrappedProcessor.process(ordinal, inbox);
    }

    @Override
    public boolean tryProcess() {
        return wrappedProcessor.tryProcess();
    }

    @Override
    public boolean complete() {
        return wrappedProcessor.complete();
    }

    @Override
    public boolean saveToSnapshot() {
        return wrappedProcessor.saveToSnapshot();
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Inbox inbox) {
        wrappedProcessor.restoreFromSnapshot(inbox);
    }

    @Override
    public boolean completeEdge(int ordinal) {
        return wrappedProcessor.completeEdge(ordinal);
    }

    @Override
    public boolean finishSnapshotRestore() {
        return wrappedProcessor.finishSnapshotRestore();
    }

    private final class ThrottlingOutbox implements Outbox {
        private final Outbox wrappedOutbox;
        private long emitCount;
        private long ts;

        private ThrottlingOutbox(Outbox wrappedOutbox) {
            this.wrappedOutbox = wrappedOutbox;
        }

        @Override
        public int bucketCount() {
            return wrappedOutbox.bucketCount();
        }

        @Override
        public boolean offer(int ordinal, @Nonnull Object item) {
            long currentTs = System.nanoTime();
            long elapsed = currentTs - ts;
            if (TimeUnit.NANOSECONDS.toSeconds(elapsed) >= 1) {
                ts = currentTs;
                emitCount = 0;
            } else {
                double emitRate = TimeUnit.SECONDS.toNanos(1) * (double) emitCount / elapsed;
                if (emitRate > itemsPerSecond) {
                    return false;
                }
            }
            boolean offered = wrappedOutbox.offer(ordinal, item);
            if (offered) {
                emitCount++;
            }
            return offered;
        }

        @Override
        public boolean offer(int[] ordinals, @Nonnull Object item) {
            return wrappedOutbox.offer(ordinals, item);
        }

        @Override
        public boolean offerToSnapshot(@Nonnull Object key, @Nonnull Object value) {
            return wrappedOutbox.offerToSnapshot(key, value);
        }
    }
}
