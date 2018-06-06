/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;

import javax.annotation.Nonnull;
import java.util.BitSet;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.execution.init.ExecutionPlan.createLoggerName;

/**
 * Internal API, see
 * {@link com.hazelcast.jet.core.processor.DiagnosticProcessors}.
 */
public final class PeekWrappedP<T> extends ProcessorWrapper {

    private final DistributedFunction<? super T, ? extends CharSequence> toStringFn;
    private final Predicate<? super T> shouldLogFn;
    private final LoggingInbox loggingInbox;
    private ILogger logger;

    private final boolean peekInput;
    private final boolean peekOutput;
    private final boolean peekSnapshot;

    private boolean peekedWatermarkLogged;

    public PeekWrappedP(
            @Nonnull Processor wrapped,
            @Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn,
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
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        logger = context.logger();
        outbox = new LoggingOutbox(outbox, peekOutput, peekSnapshot);

        // Fix issue #595: pass a logger with real class name to processor
        // We do this only if context is ProcCtx (that is, not for tests where TestProcessorContext can be used
        // and also other objects could be mocked or null, such as jetInstance())
        if (context instanceof ProcCtx) {
            ProcCtx c = (ProcCtx) context;
            NodeEngine nodeEngine = ((HazelcastInstanceImpl) c.jetInstance().getHazelcastInstance()).node.nodeEngine;
            ILogger newLogger = nodeEngine.getLogger(
                    createLoggerName(wrapped.getClass().getName(), c.vertexName(), c.globalProcessorIndex()));
            context = new ProcCtx(c.jetInstance(), c.getSerializationService(), newLogger, c.vertexName(),
                    c.localProcessorIndex(), c.globalProcessorIndex(), c.processingGuarantee(), c.localParallelism(),
                    c.memberIndex(), c.memberCount());
        }
        super.init(outbox, context);
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

    private void log(String prefix, T object) {
        // null object can come from poll()
        if (object != null && shouldLogFn.test(object)) {
            logger.info(prefix + ": " + toStringFn.apply(object));
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
            if (peekOutput) {
                logger.info("Output forwarded: " + watermark);
            }
            return true;
        }
        return false;
    }

    private class LoggingInbox implements Inbox {

        private Inbox wrappedInbox;

        private boolean peekedItemLogged;
        private int ordinal;

        @Override
        public boolean isEmpty() {
            return wrappedInbox.isEmpty();
        }

        @Override
        public Object peek() {
            T res = (T) wrappedInbox.peek();
            if (!peekedItemLogged && res != null) {
                log(res);
                peekedItemLogged = true;
            }
            return res;
        }

        @Override
        public Object poll() {
            T res = (T) wrappedInbox.poll();
            if (!peekedItemLogged && res != null) {
                log(res);
            }
            peekedItemLogged = false;
            return res;
        }

        private void log(T res) {
            PeekWrappedP.this.log("Input from ordinal " + ordinal, res);
        }

        @Override
        public void remove() {
            peekedItemLogged = false;
            wrappedInbox.remove();
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
    }
}
