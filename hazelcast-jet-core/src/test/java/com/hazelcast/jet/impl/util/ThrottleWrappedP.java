/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.core.Watermark;

import javax.annotation.Nonnull;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

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
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) throws Exception {
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
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return wrappedProcessor.tryProcessWatermark(watermark);
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
        private long startTs = Long.MIN_VALUE;

        private ThrottlingOutbox(Outbox wrappedOutbox) {
            this.wrappedOutbox = wrappedOutbox;
        }

        @Override
        public int bucketCount() {
            return wrappedOutbox.bucketCount();
        }

        @Override
        public boolean offer(int ordinal, @Nonnull Object item) {
            if (startTs == Long.MIN_VALUE) {
                startTs = System.nanoTime();
            }
            long elapsed = System.nanoTime() - startTs;
            if (NANOSECONDS.toSeconds(elapsed * itemsPerSecond) <= emitCount
                    || !wrappedOutbox.offer(ordinal, item)) {
                return false;
            }
            emitCount++;
            return true;
        }

        @Override
        public boolean offer(int[] ordinals, @Nonnull Object item) {
            return wrappedOutbox.offer(ordinals, item);
        }

        @Override
        public boolean offerToSnapshot(@Nonnull Object key, @Nonnull Object value) {
            return wrappedOutbox.offerToSnapshot(key, value);
        }

        @Override
        public boolean hasUnfinishedItem() {
            return wrappedOutbox.hasUnfinishedItem();
        }
    }
}
