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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.core.SnapshotOutbox;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.function.Predicate;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A wrapper processor to peek at input or output of other processor.
 * See {@link DiagnosticProcessors#peekInput(DistributedSupplier)}.
 */
public final class PeekWrappedP implements Processor {

    private final Processor wrappedProcessor;
    private final DistributedFunction<Object, String> toStringFn;
    private final Predicate<Object> shouldLogFn;
    private final boolean peekInput;
    private final boolean peekOutput;

    private final LoggingInbox loggingInbox;
    private ILogger logger;

    public PeekWrappedP(Processor wrappedProcessor, DistributedFunction<Object, String> toStringFn,
            Predicate<Object> shouldLogFn, boolean peekInput, boolean peekOutput
    ) {
        if (!peekInput && !peekOutput) {
            throw new IllegalArgumentException("Peeking neither on input nor on output");
        }
        checkNotNull(wrappedProcessor, "wrappedProcessor");

        this.wrappedProcessor = wrappedProcessor;
        this.toStringFn = toStringFn;
        this.shouldLogFn = shouldLogFn;
        this.peekInput = peekInput;
        this.peekOutput = peekOutput;

        loggingInbox = peekInput ? new LoggingInbox() : null;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull SnapshotOutbox snapshotOutbox, @Nonnull Context context) {
        logger = context.logger();
        if (peekOutput) {
            outbox = new LoggingOutbox(outbox, snapshotOutbox);
        }
        wrappedProcessor.init(outbox, snapshotOutbox, context);
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

    private void log(Object object) {
        // null object can come from poll()
        if (object != null && shouldLogFn.test(object)) {
            logger.info(toStringFn.apply(object));
        }
    }

    @Override
    public boolean completeEdge(int ordinal) {
        return wrappedProcessor.completeEdge(ordinal);
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
    public boolean finishSnapshotRestore() {
        return wrappedProcessor.finishSnapshotRestore();
    }

    private class LoggingInbox implements Inbox {

        private Inbox wrappedInbox;

        /** A flag, whether the last peeked item was already logged */
        private boolean wasLogged;

        @Override
        public boolean isEmpty() {
            return wrappedInbox.isEmpty();
        }

        @Override
        public Object peek() {
            Object res = wrappedInbox.peek();
            if (!wasLogged && res != null) {
                log(res);
                wasLogged = true;
            }
            return res;
        }

        @Override
        public Object poll() {
            Object res = wrappedInbox.poll();
            if (!wasLogged && res != null) {
                log(res);
            }
            wasLogged = false;
            return res;
        }

        @Override
        public Object remove() {
            wasLogged = false;
            return wrappedInbox.remove();
        }
    }

    private final class LoggingOutbox implements Outbox, SnapshotOutbox {
        private final Outbox wrappedOutbox;
        private final SnapshotOutbox snapshotOutbox;

        private LoggingOutbox(Outbox wrappedOutbox, SnapshotOutbox snapshotOutbox) {
            this.wrappedOutbox = wrappedOutbox;
            this.snapshotOutbox = snapshotOutbox;
        }

        @Override
        public int bucketCount() {
            return wrappedOutbox.bucketCount();
        }

        @Override
        public boolean offer(int ordinal, @Nonnull Object item) {
            if (wrappedOutbox.offer(ordinal, item)) {
                log(item);
                return true;
            }
            return false;
        }

        @Override
        public boolean offer(int[] ordinals, @Nonnull Object item) {
            if (wrappedOutbox.offer(ordinals, item)) {
                log(item);
                return true;
            }
            return false;
        }

        @Override
        public boolean offer(Object key, Object value) {
            //TODO: logging
            return snapshotOutbox.offer(key, value);
        }

        @Override
        public boolean offerBroadcast(Object key, Object value) {
            //TODO: logging
            return snapshotOutbox.offerBroadcast(key, value);
        }
    }
}
