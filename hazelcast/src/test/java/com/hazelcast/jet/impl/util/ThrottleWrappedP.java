/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.processor.ProcessorWrapper;

import javax.annotation.Nonnull;
import java.util.function.BooleanSupplier;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A wrapper processor to throttle the output of a processor.
 */
public final class ThrottleWrappedP extends ProcessorWrapper {

    private final long itemsPerSecond;

    public ThrottleWrappedP(Processor wrappedProcessor, long itemsPerSecond) {
        super(wrappedProcessor);
        checkNotNull(wrappedProcessor, "wrappedProcessor");
        checkTrue(wrappedProcessor.isCooperative(), "wrappedProcessor must be cooperative");

        this.itemsPerSecond = itemsPerSecond;
    }

    @Override
    protected Outbox wrapOutbox(Outbox outbox) {
        return new ThrottlingOutbox(outbox);
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
            return offerInternal(() -> wrappedOutbox.offer(ordinal, item));
        }

        @Override
        public boolean offer(@Nonnull int[] ordinals, @Nonnull Object item) {
            return offerInternal(() -> wrappedOutbox.offer(ordinals, item));
        }

        private boolean offerInternal(BooleanSupplier action) {
            if (startTs == Long.MIN_VALUE) {
                startTs = System.nanoTime();
            }
            long elapsed = System.nanoTime() - startTs;
            if (NANOSECONDS.toSeconds(elapsed * itemsPerSecond) <= emitCount
                    || !action.getAsBoolean()) {
                return false;
            }
            emitCount++;
            return true;
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
