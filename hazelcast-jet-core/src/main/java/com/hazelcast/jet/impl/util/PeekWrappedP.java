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

import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.function.Predicate;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A wrapper processor to peek at input or output of other processor.
 * See {@link com.hazelcast.jet.Processors#peekInput(DistributedSupplier)}.
 */
public final class PeekWrappedP implements Processor {

    private final Processor wrappedProcessor;
    private final DistributedFunction<Object, String> toStringF;
    private final Predicate<Object> shouldLogF;
    private final boolean peekInput;
    private final boolean peekOutput;

    private final LoggingInbox loggingInbox;
    private ILogger logger;

    public PeekWrappedP(Processor wrappedProcessor, DistributedFunction<Object, String> toStringF,
            Predicate<Object> shouldLogF, boolean peekInput, boolean peekOutput
    ) {
        if (!peekInput && !peekOutput) {
            throw new IllegalArgumentException("Peeking neither on input nor on output");
        }
        checkNotNull(wrappedProcessor, "wrappedProcessor");

        this.wrappedProcessor = wrappedProcessor;
        this.toStringF = toStringF;
        this.shouldLogF = shouldLogF;
        this.peekInput = peekInput;
        this.peekOutput = peekOutput;

        loggingInbox = peekInput ? new LoggingInbox() : null;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        logger = context.logger();
        if (peekOutput) {
            outbox = new LoggingOutbox(outbox);
        }
        wrappedProcessor.init(outbox, context);
    }

    @Override
    public boolean isCooperative() {
        return wrappedProcessor.isCooperative();
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (peekInput) {
            loggingInbox.wrappedInbox = inbox;
            wrappedProcessor.process(ordinal, loggingInbox);
        } else {
            wrappedProcessor.process(ordinal, inbox);
        }
    }

    @Override
    public boolean tryProcess() {
        return wrappedProcessor.tryProcess();
    }

    @Override
    public boolean complete() {
        return wrappedProcessor.complete();
    }

    private boolean log(Object object) {
        if (shouldLogF.test(object)) {
            logger.info(toStringF.apply(object));
        }
        return true;
    }

    private class LoggingInbox implements Inbox {

        private Inbox wrappedInbox;

        @Override
        public boolean isEmpty() {
            return wrappedInbox.isEmpty();
        }

        @Override
        public Object peek() {
            return wrappedInbox.peek();
        }

        @Override
        public Object poll() {
            return log(wrappedInbox.poll());
        }

        @Override
        public Object remove() {
            return log(wrappedInbox.remove());
        }
    }

    private final class LoggingOutbox implements Outbox {
        private final Outbox wrappedOutbox;

        private LoggingOutbox(Outbox wrappedOutbox) {
            this.wrappedOutbox = wrappedOutbox;
        }

        @Override
        public int bucketCount() {
            return wrappedOutbox.bucketCount();
        }

        @Override
        public boolean offer(int ordinal, @Nonnull Object item) {
            return wrappedOutbox.offer(ordinal, item)
                    && log(item);
        }

        @Override
        public boolean offer(int[] ordinals, @Nonnull Object item) {
            return wrappedOutbox.offer(ordinals, item)
                    && log(item);
        }
    }
}
