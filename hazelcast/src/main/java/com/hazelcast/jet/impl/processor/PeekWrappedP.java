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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.Util.toLocalTime;

/**
 * Internal API, see {@link DiagnosticProcessors}.
 */
public final class PeekWrappedP<T> extends ProcessorWrapper {

    private final FunctionEx<? super T, ? extends CharSequence> toStringFn;
    private final Predicate<? super T> shouldLogFn;
    private final LoggingInbox loggingInbox;
    private ILogger logger;

    private final boolean peekInput;
    private final boolean peekOutput;
    private final boolean peekSnapshot;

    private boolean peekedWatermarkLogged;

    public PeekWrappedP(
            @Nonnull Processor wrapped,
            @Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn,
            @Nonnull Predicate<? super T> shouldLogFn,
            boolean peekInput, boolean peekOutput, boolean peekSnapshot
    ) {
        super(wrapped);
        this.toStringFn = toStringFn;
        this.shouldLogFn = shouldLogFn;
        this.peekInput = peekInput;
        this.peekOutput = peekOutput;
        this.peekSnapshot = peekSnapshot;
        loggingInbox = peekInput ? new LoggingInbox() : null;
    }

    @Override
    protected Outbox wrapOutbox(Outbox outbox) {
        return new LoggingOutbox(outbox, peekOutput, peekSnapshot);
    }

    @Override
    protected void initWrapper(@Nonnull Outbox outbox, @Nonnull Context context) {
        logger = context.logger();
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        if (peekInput) {
            loggingInbox.wrappedInbox = inbox;
            loggingInbox.ordinal = ordinal;
            super.process(ordinal, loggingInbox);
        } else {
            super.process(ordinal, inbox);
        }
    }

    private void log(String prefix, @Nonnull T object) {
        if (shouldLogFn.test(object)) {
            logger.info(prefix + ": " + toStringFn.apply(object)
                    + (object instanceof JetEvent
                            ? " (eventTime=" + toLocalTime(((JetEvent) object).timestamp()) + ")"
                            : ""));
        }
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        if (peekInput && !peekedWatermarkLogged) {
            logger.info("Input: " + watermark);
            peekedWatermarkLogged = true;
        }
        if (super.tryProcessWatermark(watermark)) {
            peekedWatermarkLogged = false;
            return true;
        }
        return false;
    }

    private class LoggingInbox implements Inbox {

        private Inbox wrappedInbox;

        private int ordinal;

        @Override
        public boolean isEmpty() {
            return wrappedInbox.isEmpty();
        }

        @Override
        public Object peek() {
            return wrappedInbox.peek();
        }

        @Nonnull @Override
        public Iterator<Object> iterator() {
            Iterator<Object> it = wrappedInbox.iterator();
            return new Iterator<Object>() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Object next() {
                    return it.next();
                }
            };
        }

        @Override
        public Object poll() {
            @SuppressWarnings("unchecked")
            T res = (T) wrappedInbox.poll();
            if (res != null) {
                log(res);
            }
            return res;
        }

        private void log(@Nonnull T res) {
            PeekWrappedP.this.log("Input from ordinal " + ordinal, res);
        }

        @Override
        public void remove() {
            if (poll() == null) {
                throw new NoSuchElementException();
            }
        }

        @Override
        @SuppressWarnings("EmptyBlock")
        public void clear() {
            while (poll() != null) { }
        }

        @Override
        public int size() {
            return wrappedInbox.size();
        }
    }

    private final class LoggingOutbox implements Outbox {
        private final Outbox wrappedOutbox;
        private final int[] all;
        private final boolean logOutput;
        private final boolean logSnapshot;
        private final BitSet broadcastTracker;


        private LoggingOutbox(Outbox wrappedOutbox, boolean logOutput, boolean logSnapshot) {
            this.wrappedOutbox = wrappedOutbox;
            this.broadcastTracker = new BitSet(wrappedOutbox.bucketCount());
            this.all = IntStream.range(0, wrappedOutbox.bucketCount()).toArray();
            this.logOutput = logOutput;
            this.logSnapshot = logSnapshot;
        }

        @Override
        public int bucketCount() {
            return wrappedOutbox.bucketCount();
        }

        @Override
        public boolean offer(int ordinal, @Nonnull Object item) {
            if (ordinal == -1) {
                return offer(all, item);
            }

            if (!wrappedOutbox.offer(ordinal, item)) {
                return false;
            }
            if (logOutput) {
                String prefix = "Output to ordinal " + ordinal;
                if (item instanceof Watermark) {
                    logger.info(prefix + ": " + item);
                } else {
                    log(prefix, (T) item);
                }
            }
            return true;
        }

        @Override
        public boolean offer(int[] ordinals, @Nonnull Object item) {
            // use broadcast logic to be able to report accurately
            // which queue was pushed to when.
            for (int i = 0; i < ordinals.length; i++) {
                if (broadcastTracker.get(i)) {
                    continue;
                }
                if (offer(i, item)) {
                    broadcastTracker.set(i);
                } else {
                    return false;
                }
            }
            broadcastTracker.clear();
            return true;
        }

        @Override
        public boolean offerToSnapshot(@Nonnull Object key, @Nonnull Object value) {
            if (!wrappedOutbox.offerToSnapshot(key, value)) {
                return false;
            }
            if (logSnapshot) {
                log("Output to snapshot", (T) entry(key, value));
            }
            return true;
        }

        @Override
        public boolean hasUnfinishedItem() {
            return wrappedOutbox.hasUnfinishedItem();
        }
    }
}
