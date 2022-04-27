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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.MutableInteger;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.internal.util.collection.FixedCapacityArrayList;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.processor.ProcessorWrapper;
import com.hazelcast.jet.impl.util.ArrayDequeInbox;
import com.hazelcast.jet.impl.util.CircularListCursor;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Predicate;

import static com.hazelcast.jet.core.metrics.MetricNames.COALESCED_WM;
import static com.hazelcast.jet.core.metrics.MetricNames.EMITTED_COUNT;
import static com.hazelcast.jet.core.metrics.MetricNames.LAST_FORWARDED_WM;
import static com.hazelcast.jet.core.metrics.MetricNames.LAST_FORWARDED_WM_LATENCY;
import static com.hazelcast.jet.core.metrics.MetricNames.RECEIVED_BATCHES;
import static com.hazelcast.jet.core.metrics.MetricNames.RECEIVED_COUNT;
import static com.hazelcast.jet.core.metrics.MetricNames.TOP_OBSERVED_WM;
import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.execution.ProcessorState.CLOSE;
import static com.hazelcast.jet.impl.execution.ProcessorState.COMPLETE;
import static com.hazelcast.jet.impl.execution.ProcessorState.COMPLETE_EDGE;
import static com.hazelcast.jet.impl.execution.ProcessorState.EMIT_BARRIER;
import static com.hazelcast.jet.impl.execution.ProcessorState.EMIT_DONE_ITEM;
import static com.hazelcast.jet.impl.execution.ProcessorState.END;
import static com.hazelcast.jet.impl.execution.ProcessorState.NULLARY_PROCESS;
import static com.hazelcast.jet.impl.execution.ProcessorState.PRE_EMIT_DONE_ITEM;
import static com.hazelcast.jet.impl.execution.ProcessorState.PROCESS_INBOX;
import static com.hazelcast.jet.impl.execution.ProcessorState.PROCESS_WATERMARK;
import static com.hazelcast.jet.impl.execution.ProcessorState.SAVE_SNAPSHOT;
import static com.hazelcast.jet.impl.execution.ProcessorState.SNAPSHOT_COMMIT_FINISH__COMPLETE;
import static com.hazelcast.jet.impl.execution.ProcessorState.SNAPSHOT_COMMIT_FINISH__FINAL;
import static com.hazelcast.jet.impl.execution.ProcessorState.SNAPSHOT_COMMIT_FINISH__PROCESS;
import static com.hazelcast.jet.impl.execution.ProcessorState.SNAPSHOT_COMMIT_PREPARE;
import static com.hazelcast.jet.impl.execution.ProcessorState.WAITING_FOR_SNAPSHOT_COMPLETED;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static com.hazelcast.jet.impl.execution.WatermarkCoalescer.NO_NEW_WM;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.PrefixedLogger.prefix;
import static com.hazelcast.jet.impl.util.PrefixedLogger.prefixedLogger;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static com.hazelcast.jet.impl.util.Util.doWithClassLoader;
import static com.hazelcast.jet.impl.util.Util.jobNameAndExecutionId;
import static com.hazelcast.jet.impl.util.Util.lazyAdd;
import static com.hazelcast.jet.impl.util.Util.lazyIncrement;
import static com.hazelcast.jet.impl.util.Util.sum;
import static java.util.Comparator.comparing;

public class ProcessorTasklet implements Tasklet {

    private static final int OUTBOX_BATCH_SIZE = 2048;

    private final ProgressTracker progTracker = new ProgressTracker();
    private final OutboundEdgeStream[] outstreams;
    private final OutboxImpl outbox;
    private final Processor.Context context;

    private final SnapshotContext ssContext;
    private final BitSet receivedBarriers; // indicates if current snapshot is received on the ordinal

    private final ArrayDequeInbox inbox = new ArrayDequeInbox(progTracker);
    private final Queue<InboundEdgeStream[]> instreamGroupQueue;
    private final WatermarkCoalescer watermarkCoalescer;
    private final ILogger logger;
    private final SerializationService serializationService;
    private final List<? extends InboundEdgeStream> instreams;
    private final ExecutorService executionService;
    private final boolean isSource;

    private Processor processor;
    private int numActiveOrdinals; // counter for remaining active ordinals
    private CircularListCursor<InboundEdgeStream> instreamCursor;
    private InboundEdgeStream currInstream;
    private ProcessorState state;

    // pending snapshot IDs are the IDs of the next expected snapshot IDs for phase 1 and 2
    private long pendingSnapshotId1;
    private long pendingSnapshotId2;

    private SnapshotBarrier currentBarrier;
    private Watermark pendingWatermark;

    // Tells whether we are operating in exactly-once or at-least-once mode.
    // In other words, whether a barrier from all inputs must be present before
    // draining more items from an input stream where a barrier has been reached.
    // Once a terminal snapshot barrier is reached, this is always true.
    private boolean waitForAllBarriers;

    private final AtomicLongArray receivedCounts;
    private final AtomicLongArray receivedBatches;
    private final AtomicLongArray emittedCounts;

    @Probe(name = MetricNames.QUEUES_SIZE)
    private final Counter queuesSize = SwCounter.newSwCounter();

    @Probe(name = MetricNames.QUEUES_CAPACITY)
    private final Counter queuesCapacity = SwCounter.newSwCounter();

    private final Predicate<Object> addToInboxFunction = inbox.queue()::add;
    private Future<?> closeFuture;

    @SuppressWarnings("checkstyle:ExecutableStatementCount")
    public ProcessorTasklet(
            @Nonnull Context context,
            @Nonnull ExecutorService executionService,
            @Nonnull SerializationService serializationService,
            @Nonnull Processor processor,
            @Nonnull List<? extends InboundEdgeStream> instreams,
            @Nonnull List<? extends OutboundEdgeStream> outstreams,
            @Nonnull SnapshotContext ssContext,
            @Nullable OutboundCollector ssCollector,
            boolean isSource
    ) {
        Preconditions.checkNotNull(processor, "processor");
        this.context = context;
        this.executionService = executionService;
        this.serializationService = serializationService;
        this.processor = processor;
        this.numActiveOrdinals = instreams.size();
        this.instreams = instreams;
        this.instreamGroupQueue = createInstreamGroupQueue(instreams);
        this.outstreams = outstreams.stream()
                                    .sorted(comparing(OutboundEdgeStream::ordinal))
                                    .toArray(OutboundEdgeStream[]::new);
        this.ssContext = ssContext;
        String prefix = prefix(context.jobConfig().getName(),
                context.jobId(), context.vertexName(), context.globalProcessorIndex());
        this.logger = prefixedLogger(getLogger(context), prefix);
        this.isSource = isSource;

        instreamCursor = popInstreamGroup();
        receivedCounts = new AtomicLongArray(instreams.size());
        receivedBatches = new AtomicLongArray(instreams.size());
        emittedCounts = new AtomicLongArray(outstreams.size() + 1);
        outbox = createOutbox(ssCollector);
        receivedBarriers = new BitSet(instreams.size());
        state = processingState();
        pendingSnapshotId1 = pendingSnapshotId2 = ssContext.activeSnapshotIdPhase1() + 1;
        waitForAllBarriers = ssContext.processingGuarantee() == ProcessingGuarantee.EXACTLY_ONCE;

        watermarkCoalescer = WatermarkCoalescer.create(instreams.size());
    }

    private Queue<InboundEdgeStream[]> createInstreamGroupQueue(List<? extends InboundEdgeStream> instreams) {
        Int2ObjectHashMap<MutableInteger> priorityCounters = new Int2ObjectHashMap<>();
        for (InboundEdgeStream instream : instreams) {
            priorityCounters.computeIfAbsent(instream.priority(), priority -> new MutableInteger()).getAndInc();
        }

        Map<Integer, FixedCapacityArrayList<InboundEdgeStream>> priorityToStreams = new TreeMap<>();
        for (Map.Entry<Integer, MutableInteger> priorityWithCounter : priorityCounters.entrySet()) {
            FixedCapacityArrayList<InboundEdgeStream> streams =
                    new FixedCapacityArrayList<>(InboundEdgeStream.class, priorityWithCounter.getValue().value);
            priorityToStreams.put(priorityWithCounter.getKey(), streams);
        }

        for (InboundEdgeStream instream : instreams) {
            priorityToStreams.get(instream.priority()).add(instream);
        }

        Queue<InboundEdgeStream[]> queue = new ArrayDeque<>(priorityToStreams.size());
        for (FixedCapacityArrayList<InboundEdgeStream> streams : priorityToStreams.values()) {
            queue.add(streams.asArray());
        }

        return queue;
    }

    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE",
            justification = "hazelcastInstance() can be null in TestProcessorContext")
    private ILogger getLogger(@Nonnull Context context) {
        //noinspection ConstantConditions
        return context.hazelcastInstance() != null
                ? context.hazelcastInstance().getLoggingService().getLogger(getClass())
                : Logger.getLogger(getClass());
    }

    private OutboxImpl createOutbox(@Nullable OutboundCollector ssCollector) {
        OutboundCollector[] collectors;
        if (ssCollector != null) {
            collectors = new OutboundCollector[outstreams.length + 1];
            collectors[outstreams.length] = ssCollector;
        } else {
            collectors = new OutboundCollector[outstreams.length];
        }
        for (int i = 0; i < outstreams.length; i++) {
            collectors[i] = outstreams[i].getCollector();
        }
        return new OutboxImpl(collectors, ssCollector != null, progTracker,
                serializationService, OUTBOX_BATCH_SIZE, emittedCounts);
    }

    @Override
    public void init() {
        ManagedContext managedContext = serializationService.getManagedContext();
        if (managedContext != null) {
            Processor toInit = processor instanceof ProcessorWrapper
                    ? ((ProcessorWrapper) processor).getWrapped() : processor;
            Object initialized = null;
            try {
                initialized = managedContext.initialize(toInit);
                toInit = (Processor) initialized;
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(String.format(
                        "The initialized object(%s) should be an instance of %s", initialized, Processor.class), e);
            }
            if (processor instanceof ProcessorWrapper) {
                ((ProcessorWrapper) processor).setWrapped(toInit);
            } else {
                processor = toInit;
            }
        }
        try {
            doWithClassLoader(context.classLoader(), () -> processor.init(outbox, context));
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    @Override @Nonnull
    public ProgressState call() {
        assert state != END : "already in terminal state";
        progTracker.reset();
        progTracker.notDone();
        outbox.reset();
        stateMachineStep();
        return progTracker.toProgressState();
    }

    private void closeProcessor() {
        try {
            doWithClassLoader(context.classLoader(), () -> processor.close());
        } catch (Throwable e) {
            logger.severe(jobNameAndExecutionId(context.jobConfig().getName(), context.executionId())
                    + " encountered an exception in Processor.close(), ignoring it", e);
        }
    }

    @Override
    public Processor.Context getProcessorContext() {
        return context;
    }

    @SuppressWarnings("checkstyle:returncount")
    private void stateMachineStep() {
        switch (state) {
            case PROCESS_WATERMARK:
                if (pendingWatermark == null) {
                    long wm = watermarkCoalescer.checkWmHistory();
                    if (wm == NO_NEW_WM) {
                        state = NULLARY_PROCESS;
                        stateMachineStep(); // recursion
                        break;
                    }
                    pendingWatermark = new Watermark(wm);
                }
                if (pendingWatermark.equals(IDLE_MESSAGE)
                        ? outbox.offer(IDLE_MESSAGE)
                        : doWithClassLoader(context.classLoader(), () -> processor.tryProcessWatermark(pendingWatermark))) {
                    state = NULLARY_PROCESS;
                    pendingWatermark = null;
                }
                break;

            case NULLARY_PROCESS:
                // if currInstream is null, maybe fillInbox wasn't called yet. Avoid calling tryProcess in that case.
                if (currInstream == null || isSnapshotInbox() ||
                        doWithClassLoader(context.classLoader(), () -> processor.tryProcess())) {
                    state = PROCESS_INBOX;
                    outbox.reset();
                    stateMachineStep(); // recursion
                }
                break;

            case PROCESS_INBOX:
                processInbox();
                return;

            case COMPLETE_EDGE:
                if (isSnapshotInbox()
                        ? doWithClassLoader(context.classLoader(), () -> processor.finishSnapshotRestore())
                        : doWithClassLoader(context.classLoader(), () -> processor.completeEdge(currInstream.ordinal()))) {
                    assert !outbox.hasUnfinishedItem() || !isSnapshotInbox() :
                            "outbox has an unfinished item after successful finishSnapshotRestore()";
                    progTracker.madeProgress();
                    state = processingState();
                }
                return;

            case SAVE_SNAPSHOT:
                if (doWithClassLoader(context.classLoader(), () -> processor.saveToSnapshot())) {
                    progTracker.madeProgress();
                    state = ssContext.isExportOnly() ? EMIT_BARRIER : SNAPSHOT_COMMIT_PREPARE;
                    stateMachineStep(); // recursion
                }
                return;

            case SNAPSHOT_COMMIT_PREPARE:
                if (doWithClassLoader(context.classLoader(), () -> processor.snapshotCommitPrepare())) {
                    progTracker.madeProgress();
                    state = EMIT_BARRIER;
                    stateMachineStep(); // recursion
                }
                return;

            case EMIT_BARRIER:
                assert currentBarrier != null : "currentBarrier == null";
                if (outbox.offerToEdgesAndSnapshot(currentBarrier)) {
                    progTracker.madeProgress();
                    if (currentBarrier.isTerminal()) {
                        state = WAITING_FOR_SNAPSHOT_COMPLETED;
                    } else {
                        currentBarrier = null;
                        receivedBarriers.clear();
                        pendingSnapshotId1++;
                        state = processingState();
                    }
                }
                return;

            case SNAPSHOT_COMMIT_FINISH__PROCESS:
            case SNAPSHOT_COMMIT_FINISH__COMPLETE:
            case SNAPSHOT_COMMIT_FINISH__FINAL:
                if (ssContext.isExportOnly() ||
                        doWithClassLoader(context.classLoader(),
                                () -> processor.snapshotCommitFinish(ssContext.isLastPhase1Successful()))) {

                    pendingSnapshotId2++;
                    ssContext.phase2DoneForTasklet();
                    progTracker.madeProgress();
                    switch (state) {
                        case SNAPSHOT_COMMIT_FINISH__PROCESS:
                            state = PROCESS_INBOX;
                            break;
                        case SNAPSHOT_COMMIT_FINISH__COMPLETE:
                            state = COMPLETE;
                            break;
                        case SNAPSHOT_COMMIT_FINISH__FINAL:
                            state = PRE_EMIT_DONE_ITEM;
                            break;
                        default:
                            throw new RuntimeException("unexpected state: " + state);
                    }
                }
                return;

            case WAITING_FOR_SNAPSHOT_COMPLETED:
                long currSnapshotId2 = ssContext.activeSnapshotIdPhase2();
                if (currSnapshotId2 >= pendingSnapshotId2) {
                    state = SNAPSHOT_COMMIT_FINISH__FINAL;
                    stateMachineStep(); // recursion
                }
                return;

            case COMPLETE:
                complete();
                return;

            case PRE_EMIT_DONE_ITEM:
                ssContext.processorTaskletDone(pendingSnapshotId2 - 1);
                state = EMIT_DONE_ITEM;
                stateMachineStep();
                return;

            case EMIT_DONE_ITEM:
                if (outbox.offerToEdgesAndSnapshot(DONE_ITEM)) {
                    progTracker.madeProgress();
                    state = CLOSE;
                    stateMachineStep();
                }
                return;

            case CLOSE:
                if (isCooperative() && !processor.closeIsCooperative()) {
                    if (closeFuture == null) {
                        ClassLoader contextCl = Thread.currentThread().getContextClassLoader();
                        closeFuture = executionService.submit(() -> doWithClassLoader(contextCl, this::closeProcessor));
                        progTracker.madeProgress();
                    }
                    if (!closeFuture.isDone()) {
                        return;
                    }
                    progTracker.madeProgress();
                } else {
                    closeProcessor();
                }
                state = END;
                progTracker.done();
                return;

            default:
                // note ProcessorState.END goes here
                throw new JetException("Unexpected state: " + state);
        }
    }

    private void processInbox() {
        if (ssContext.activeSnapshotIdPhase2() == pendingSnapshotId2) {
            state = SNAPSHOT_COMMIT_FINISH__PROCESS;
            progTracker.madeProgress();
            return;
        }

        if (inbox.isEmpty()) {
            fillInbox();
        }
        if (!inbox.isEmpty()) {
            if (isSnapshotInbox()) {
                doWithClassLoader(context.classLoader(), () -> processor.restoreFromSnapshot(inbox));
            } else {
                doWithClassLoader(context.classLoader(), () -> processor.process(currInstream.ordinal(), inbox));
            }
        }

        if (inbox.isEmpty()) {
            // there is either snapshot or instream is done, not both
            if (currInstream != null && currInstream.isDone()) {
                state = COMPLETE_EDGE;
                progTracker.madeProgress();
            } else if (numActiveOrdinals > 0
                    && receivedBarriers.cardinality() == numActiveOrdinals) {
                // we have an empty inbox and received the current snapshot barrier from all active ordinals
                state = SAVE_SNAPSHOT;
            } else if (numActiveOrdinals == 0) {
                progTracker.madeProgress();
                state = COMPLETE;
            } else {
                state = PROCESS_WATERMARK;
            }
        }
    }

    private void complete() {
        // check ssContext to see if a snapshot phase should be executed
        if (pendingSnapshotId1 == pendingSnapshotId2) {
            long currSnapshotId1 = ssContext.activeSnapshotIdPhase1();
            assert currSnapshotId1 + 1 == pendingSnapshotId1 || currSnapshotId1 == pendingSnapshotId1
                    : "Unexpected new phase 1 snapshot id: " + currSnapshotId1 + ", expected was "
                    + (pendingSnapshotId1 - 1) + " or " + pendingSnapshotId1;
            if (currSnapshotId1 == pendingSnapshotId1) {
                if (outbox.hasUnfinishedItem()) {
                    outbox.block();
                } else {
                    outbox.unblock();
                    state = SAVE_SNAPSHOT;
                    currentBarrier = new SnapshotBarrier(currSnapshotId1, ssContext.isTerminalSnapshot());
                    progTracker.madeProgress();
                    return;
                }
            }
        } else {
            long currSnapshotId2 = ssContext.activeSnapshotIdPhase2();
            assert currSnapshotId2 + 1 == pendingSnapshotId2 || currSnapshotId2 == pendingSnapshotId2
                    : "Unexpected new phase 2 snapshot id: " + currSnapshotId2 + ", expected was "
                    + (pendingSnapshotId2 - 1) + " or " + pendingSnapshotId2;
            if (currSnapshotId2 == pendingSnapshotId2) {
                state = SNAPSHOT_COMMIT_FINISH__COMPLETE;
                progTracker.madeProgress();
                return;
            }
        }
        if (processor.complete()) {
            progTracker.madeProgress();
            state = pendingSnapshotId2 < pendingSnapshotId1
                    ? WAITING_FOR_SNAPSHOT_COMPLETED
                    : PRE_EMIT_DONE_ITEM;
        }
    }

    private void fillInbox() {
        assert inbox.isEmpty() : "inbox is not empty";
        assert pendingWatermark == null : "null wm expected, but was " + pendingWatermark;

        // We need to collect metrics before draining the queues into Inbox,
        // otherwise they would appear empty even for slow processors
        queuesCapacity.set(instreamCursor == null ? 0 :
                sum(instreamCursor.getArray(), InboundEdgeStream::capacities, instreamCursor.getSize()));
        queuesSize.set(instreamCursor == null ? 0 :
                sum(instreamCursor.getArray(), InboundEdgeStream::sizes, instreamCursor.getSize()));

        if (instreamCursor == null) {
            return;
        }

        final InboundEdgeStream first = instreamCursor.value();
        ProgressState result;
        do {
            currInstream = instreamCursor.value();
            result = NO_PROGRESS;

            // skip ordinals where a snapshot barrier has already been received
            if (waitForAllBarriers && receivedBarriers.get(currInstream.ordinal())) {
                instreamCursor.advance();
                continue;
            }
            result = currInstream.drainTo(addToInboxFunction);
            progTracker.madeProgress(result.isMadeProgress());

            // check if the last drained item is special
            Object lastItem = inbox.queue().peekLast();
            if (lastItem instanceof Watermark) {
                long newWmValue = ((Watermark) inbox.queue().removeLast()).timestamp();
                long wm = watermarkCoalescer.observeWm(currInstream.ordinal(), newWmValue);
                if (wm != NO_NEW_WM) {
                    pendingWatermark = new Watermark(wm);
                }
            } else if (lastItem instanceof SnapshotBarrier) {
                SnapshotBarrier barrier = (SnapshotBarrier) inbox.queue().removeLast();
                observeBarrier(currInstream.ordinal(), barrier);
            } else if (lastItem != null && !(lastItem instanceof BroadcastItem)) {
                watermarkCoalescer.observeEvent(currInstream.ordinal());
            }

            if (result.isDone()) {
                receivedBarriers.clear(currInstream.ordinal());
                long wm = watermarkCoalescer.queueDone(currInstream.ordinal());
                // Note that there can be a WM received from upstream and the result can be done after single drain.
                // In this case we might overwrite the WM here, but that's fine since the second WM should be newer.
                if (wm != NO_NEW_WM) {
                    assert pendingWatermark == null || pendingWatermark.timestamp() < wm
                            : "trying to assign lower WM. Old=" + pendingWatermark.timestamp() + ", new=" + wm;
                    pendingWatermark = new Watermark(wm);
                }
                instreamCursor.remove();
                numActiveOrdinals--;
            }

            // pop current priority group
            if (!instreamCursor.advance()) {
                instreamCursor = popInstreamGroup();
                break;
            }
        } while (!result.isMadeProgress() && instreamCursor.value() != first);

        // we are the only updating thread, no need for CAS operations
        lazyAdd(receivedCounts, currInstream.ordinal(), inbox.size());
        if (!inbox.isEmpty()) {
            lazyIncrement(receivedBatches, currInstream.ordinal());
        }
    }

    private CircularListCursor<InboundEdgeStream> popInstreamGroup() {
        return Optional.ofNullable(instreamGroupQueue.poll())
                       .map(CircularListCursor::new)
                       .orElse(null);
    }

    @Override
    public String toString() {
        String prefix = prefix(context.jobConfig().getName(),
                context.jobId(), context.vertexName(), context.globalProcessorIndex());
        return "ProcessorTasklet{" + prefix + '}';
    }

    private void observeBarrier(int ordinal, SnapshotBarrier barrier) {
        if (barrier.snapshotId() != pendingSnapshotId1) {
            throw new JetException("Unexpected snapshot barrier ID " + barrier.snapshotId() + " from ordinal " + ordinal +
                    ", expected " + pendingSnapshotId1);
        }
        currentBarrier = barrier;
        if (barrier.isTerminal()) {
            // Switch to exactly-once mode. The reason is that there will be DONE_ITEM just after the
            // terminal barrier and if we process it before receiving the other barriers, it could cause
            // the watermark to advance. The exactly-once mode disallows processing of any items after
            // the barrier before the barrier is processed.
            waitForAllBarriers = true;
        }
        receivedBarriers.set(ordinal);
    }

    /**
     * If there are no inbound ordinals left, we will go to COMPLETE state
     * otherwise to PROCESS_WATERMARK.
     */
    private ProcessorState processingState() {
        return instreamCursor == null ? COMPLETE : PROCESS_WATERMARK;
    }

    /**
     * Returns, if the inbox we are currently on is the snapshot restoring inbox.
     */
    private boolean isSnapshotInbox() {
        return currInstream != null && currInstream.priority() == Integer.MIN_VALUE;
    }

    private long lastForwardedWmLatency() {
        long wm = outbox.lastForwardedWm();
        if (wm == IDLE_MESSAGE.timestamp()) {
            return Long.MIN_VALUE; // idle
        }
        if (wm == Long.MIN_VALUE) {
            return Long.MAX_VALUE; // no wms emitted
        }
        return System.currentTimeMillis() - wm;
    }

    @Override
    public boolean isCooperative() {
        return doWithClassLoader(context.classLoader(), () -> processor.isCooperative());
    }

    @Override
    public void close() {
        if (state == CLOSE) {
            try {
                closeFuture.get();
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        } else if (state != END) {
            closeProcessor();
        }
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext mContext) {
        descriptor = descriptor.withTag(MetricTags.VERTEX, this.context.vertexName())
                       .withTag(MetricTags.PROCESSOR_TYPE, this.processor.getClass().getSimpleName())
                       .withTag(MetricTags.PROCESSOR, Integer.toString(this.context.globalProcessorIndex()));

        if (isSource) {
            descriptor = descriptor.withTag(MetricTags.SOURCE, "true");
        }
        if (outstreams.length == 0) {
            descriptor = descriptor.withTag(MetricTags.SINK, "true");
        }

        for (int i = 0; i < instreams.size(); i++) {
            MetricDescriptor descWithOrdinal = descriptor.copy().withTag(MetricTags.ORDINAL, String.valueOf(i));
            mContext.collect(descWithOrdinal, RECEIVED_COUNT, ProbeLevel.INFO, ProbeUnit.COUNT, receivedCounts.get(i));
            mContext.collect(descWithOrdinal, RECEIVED_BATCHES, ProbeLevel.INFO, ProbeUnit.COUNT, receivedBatches.get(i));
        }

        for (int i = 0; i < emittedCounts.length() - (this.context.snapshottingEnabled() ? 0 : 1); i++) {
            String ordinal = i == emittedCounts.length() - 1 ? "snapshot" : String.valueOf(i);
            MetricDescriptor descriptorWithOrdinal = descriptor.copy().withTag(MetricTags.ORDINAL, ordinal);
            mContext.collect(descriptorWithOrdinal, EMITTED_COUNT, ProbeLevel.INFO, ProbeUnit.COUNT, emittedCounts.get(i));
        }

        mContext.collect(descriptor, TOP_OBSERVED_WM, ProbeLevel.INFO, ProbeUnit.MS, watermarkCoalescer.topObservedWm());
        mContext.collect(descriptor, COALESCED_WM, ProbeLevel.INFO, ProbeUnit.MS, watermarkCoalescer.coalescedWm());
        mContext.collect(descriptor, LAST_FORWARDED_WM, ProbeLevel.INFO, ProbeUnit.MS, outbox.lastForwardedWm());
        mContext.collect(descriptor, LAST_FORWARDED_WM_LATENCY, ProbeLevel.INFO, ProbeUnit.MS, lastForwardedWmLatency());

        mContext.collect(descriptor, this);

        //collect static metrics from processor
        mContext.collect(descriptor, this.processor);
        //collect dynamic metrics from processor
        if (processor instanceof DynamicMetricsProvider) {
            ((DynamicMetricsProvider) processor).provideDynamicMetrics(descriptor.copy(), mContext);
        }
        if (context instanceof ProcCtx) {
            ((ProcCtx) context).metricsContext().provideDynamicMetrics(descriptor, mContext);
        }
    }
}
