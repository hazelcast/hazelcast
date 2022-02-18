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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.JobClassLoaderService;
import com.hazelcast.jet.impl.TerminationMode;
import com.hazelcast.jet.impl.exception.JobTerminateRequestedException;
import com.hazelcast.jet.impl.exception.TerminatedWithSnapshotException;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.execution.init.VertexDef;
import com.hazelcast.jet.impl.metrics.RawJobMetrics;
import com.hazelcast.jet.impl.operation.SnapshotPhase1Operation.SnapshotPhase1Result;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.metrics.MetricNames.EXECUTION_COMPLETION_TIME;
import static com.hazelcast.jet.core.metrics.MetricNames.EXECUTION_START_TIME;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.doWithClassLoader;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;

/**
 * Data pertaining to single job execution on all cluster members. There's one
 * instance per job execution; if the job is restarted, another instance will
 * be used.
 */
public class ExecutionContext implements DynamicMetricsProvider {

    private static final Function<? super SenderReceiverKey, ? extends Queue<byte[]>> CREATE_RECEIVER_QUEUE_FN =
            key -> new MPSCQueue<>(null);

    private final long jobId;
    private final long executionId;
    private final boolean isLightJob;
    private volatile Address coordinator;
    private volatile Set<Address> participants;
    private final long createdOn = System.nanoTime();
    private final Object executionLock = new Object();
    private final ILogger logger;
    private final Counter startTime = MwCounter.newMwCounter(-1);
    private final Counter completionTime = MwCounter.newMwCounter(-1);

    // key: resource identifier
    // we use ConcurrentHashMap because ConcurrentMap doesn't guarantee that computeIfAbsent
    // executes the supplier strictly only if it's needed.
    private final ConcurrentHashMap<String, File> tempDirectories = new ConcurrentHashMap<>();

    private String jobName;

    private volatile Map<SenderReceiverKey, ReceiverTasklet> receiverMap;
    private volatile Map<SenderReceiverKey, SenderTasklet> senderMap;
    private final Map<SenderReceiverKey, Queue<byte[]>> receiverQueuesMap;

    private List<VertexDef> vertices = emptyList();
    private List<Tasklet> tasklets = emptyList();

    // future which is completed only after all tasklets are completed and contains execution result
    private volatile CompletableFuture<Void> executionFuture;

    // future which can only be used to cancel the local execution.
    private final CompletableFuture<Void> cancellationFuture = new CompletableFuture<>();

    private final NodeEngineImpl nodeEngine;
    private final JetServiceBackend jetServiceBackend;
    private volatile SnapshotContext snapshotContext;
    private JobConfig jobConfig;

    private boolean metricsEnabled;
    private volatile RawJobMetrics jobMetrics = RawJobMetrics.empty();

    private InternalSerializationService serializationService;
    private final AtomicBoolean executionCompleted = new AtomicBoolean();

    public ExecutionContext(NodeEngineImpl nodeEngine, long jobId, long executionId, boolean isLightJob) {
        this.jobId = jobId;
        this.executionId = executionId;
        this.isLightJob = isLightJob;
        this.nodeEngine = nodeEngine;
        this.jetServiceBackend = nodeEngine.getService(JetServiceBackend.SERVICE_NAME);

        this.jobName = idToString(jobId);

        this.logger = nodeEngine.getLogger(getClass());

        // The map is only concurrent for light jobs because they can receive packets before they are
        // initialized. For regular jobs we use non-concurrent map for performance
        // We considered replacing the CHM with a HM after initialization for performance, but there's
        // not much benefit in that according to this: https://stackoverflow.com/a/32141829/952135
        receiverQueuesMap = isLightJob ? new ConcurrentHashMap<>() : new HashMap<>();
    }

    public ExecutionContext initialize(
            @Nonnull Address coordinator,
            @Nonnull Set<Address> participants,
            @Nonnull ExecutionPlan plan
    ) {
        this.coordinator = coordinator;
        this.participants = participants;

        jobConfig = plan.getJobConfig();
        jobName = jobConfig.getName() == null ? jobName : jobConfig.getName();

        // Must be populated early, so all processor suppliers are
        // available to be completed in the case of init failure
        vertices = plan.getVertices();
        snapshotContext = new SnapshotContext(nodeEngine.getLogger(SnapshotContext.class), jobNameAndExecutionId(),
                plan.lastSnapshotId(), jobConfig.getProcessingGuarantee());

        JetServiceBackend jetServiceBackend = nodeEngine.getService(JetServiceBackend.SERVICE_NAME);
        serializationService = jetServiceBackend.createSerializationService(jobConfig.getSerializerConfigs());

        metricsEnabled = jobConfig.isMetricsEnabled() && nodeEngine.getConfig().getMetricsConfig().isEnabled();
        plan.initialize(nodeEngine, jobId, executionId, snapshotContext, tempDirectories, serializationService);
        int numPrioritySsTasklets = plan.getStoreSnapshotTaskletCount() != 0 ? plan.getHigherPriorityVertexCount() : 0;
        snapshotContext.initTaskletCount(plan.getProcessorTaskletCount(), plan.getStoreSnapshotTaskletCount(),
                numPrioritySsTasklets);
        Map<SenderReceiverKey, ReceiverTasklet> receiverMapTmp = new HashMap<>();
        for (Entry<Integer, Map<Integer, Map<Address, ReceiverTasklet>>> vertexIdEntry : plan.getReceiverMap().entrySet()) {
            for (Entry<Integer, Map<Address, ReceiverTasklet>> ordinalEntry : vertexIdEntry.getValue().entrySet()) {
                for (Entry<Address, ReceiverTasklet> addressEntry : ordinalEntry.getValue().entrySet()) {
                    SenderReceiverKey key =
                            new SenderReceiverKey(vertexIdEntry.getKey(), ordinalEntry.getKey(), addressEntry.getKey());
                    // the queue might already exist, if some data were received for it, or it will be created now
                    Queue<byte[]> queue = receiverQueuesMap.computeIfAbsent(key, CREATE_RECEIVER_QUEUE_FN);
                    ReceiverTasklet receiverTasklet = addressEntry.getValue();
                    receiverTasklet.initIncomingQueue(queue);
                    receiverMapTmp.put(
                            new SenderReceiverKey(vertexIdEntry.getKey(), ordinalEntry.getKey(), addressEntry.getKey()),
                            receiverTasklet);
                }
            }
        }
        this.receiverMap = unmodifiableMap(receiverMapTmp);

        Map<SenderReceiverKey, SenderTasklet> senderMapTmp = new HashMap<>();
        for (Entry<Integer, Map<Integer, Map<Address, SenderTasklet>>> e1 : plan.getSenderMap().entrySet()) {
            for (Entry<Integer, Map<Address, SenderTasklet>> e2 : e1.getValue().entrySet()) {
                for (Entry<Address, SenderTasklet> e3 : e2.getValue().entrySet()) {
                    senderMapTmp.put(new SenderReceiverKey(e1.getKey(), e2.getKey(), e3.getKey()), e3.getValue());
                }
            }
        }
        this.senderMap = unmodifiableMap(senderMapTmp);

        tasklets = plan.getTasklets();
        return this;
    }

    /**
     * Starts local execution of job by submitting tasklets to execution service. If
     * execution was cancelled earlier then execution will not be started.
     * <p>
     * Returns a future which is completed only when all tasklets are completed. If
     * execution was already cancelled before this method is called then the returned
     * future is completed immediately. The future returned can't be cancelled,
     * instead {@link #terminateExecution} should be used.
     */
    public CompletableFuture<Void> beginExecution(TaskletExecutionService taskletExecService) {
        synchronized (executionLock) {
            if (executionFuture != null) {
                // beginExecution was already called or execution was cancelled before it started.
                LoggingUtil.logFine(logger, "%s: execution started after cancelled", jobNameAndExecutionId());
                return executionFuture;
            } else {
                // begin job execution
                ClassLoader cl = jetServiceBackend.getJobClassLoaderService().getClassLoader(jobId);
                if (cl == null) {
                    cl = nodeEngine.getConfigClassLoader();
                }
                executionFuture = taskletExecService
                        .beginExecute(tasklets, cancellationFuture, cl)
                        .whenComplete(withTryCatch(logger, (r, t) -> setCompletionTime()))
                        .thenApply(res -> {
                            // There's a race here: a snapshot could be requested after the job just completed
                            // normally, in that case we'll report that it terminated with snapshot.
                            // We ignore this for now.
                            if (snapshotContext.isTerminalSnapshot()) {
                                throw new TerminatedWithSnapshotException();
                            }
                            return res;
                        });
                startTime.set(System.currentTimeMillis());
            }
            return executionFuture;
        }
    }

    /**
     * Complete local execution. If local execution was started, it should be
     * called after execution has completed.
     */
    public void completeExecution(Throwable error) {
        assert executionFuture == null || executionFuture.isDone()
                : "If execution was begun, then completeExecution() should not be called before execution is done.";

        if (!executionCompleted.compareAndSet(false, true)) {
            return;
        }
        for (Tasklet tasklet : tasklets) {
            try {
                tasklet.close();
            } catch (Throwable e) {
                logger.severe(jobNameAndExecutionId()
                              + " encountered an exception in Processor.close(), ignoring it", e);
            }
        }

        JobClassLoaderService jobClassloaderService = jetServiceBackend.getJobClassLoaderService();
        ClassLoader classLoader = jobClassloaderService.getClassLoader(jobId);
        doWithClassLoader(classLoader, () -> {
            for (VertexDef vertex : vertices) {
                try {
                    ClassLoader processorCl = isLightJob ?
                            null : jobClassloaderService.getProcessorClassLoader(jobId, vertex.name());
                    doWithClassLoader(processorCl, () -> vertex.processorSupplier().close(error));
                } catch (Throwable e) {
                    logger.severe(jobNameAndExecutionId()
                                  + " encountered an exception in ProcessorSupplier.close(), ignoring it", e);
                }
            }
        });

        tempDirectories.forEach((k, dir) -> {
            try {
                IOUtil.delete(dir);
            } catch (Exception e) {
                logger.warning("Failed to delete temporary directory " + dir);
            }
        });

        if (serializationService != null) {
            serializationService.dispose();
        }
    }

    /**
     * Terminates the local execution of tasklets. Returns false, if the
     * execution wasn't yet begun.
     */
    public boolean terminateExecution(@Nullable TerminationMode mode) {
        assert mode == null || !mode.isWithTerminalSnapshot()
                : "terminating with a mode that should do a terminal snapshot";

        synchronized (executionLock) {
            if (mode == null) {
                cancellationFuture.cancel(true);
            } else {
                cancellationFuture.completeExceptionally(new JobTerminateRequestedException(mode));
            }
            if (executionFuture == null) {
                // if cancelled before execution started, then assign the already completed future.
                executionFuture = cancellationFuture;
                return false;
            }
            snapshotContext.cancel();
            return true;
        }
    }

    /**
     * Starts the phase 1 of a new snapshot.
     */
    public CompletableFuture<SnapshotPhase1Result> beginSnapshotPhase1(long snapshotId, String mapName, int flags) {
        LoggingUtil.logFine(logger, "Starting snapshot %d phase 1 for %s on member", snapshotId, jobNameAndExecutionId());
        synchronized (executionLock) {
            if (cancellationFuture.isDone()) {
                throw new CancellationException();
            } else if (executionFuture != null && executionFuture.isDone()) {
                // if execution is done, there are 0 processors to take snapshot of. Therefore we're done now.
                LoggingUtil.logFine(logger, "Ignoring snapshot %d phase 1 for %s: execution completed",
                        snapshotId, jobNameAndExecutionId());
                return CompletableFuture.completedFuture(new SnapshotPhase1Result(0, 0, 0, null));
            }
            return snapshotContext.startNewSnapshotPhase1(snapshotId, mapName, flags);
        }
    }

    /**
     * Starts the phase 2 of the current snapshot.
     */
    public CompletableFuture<Void> beginSnapshotPhase2(long snapshotId, boolean success) {
        LoggingUtil.logFine(logger, "Starting snapshot %d phase 2 for %s on member", snapshotId, jobNameAndExecutionId());
        synchronized (executionLock) {
            if (cancellationFuture.isDone()) {
                throw new CancellationException();
            } else if (executionFuture != null && executionFuture.isDone()) {
                // if execution is done, there are 0 processors to take snapshot of. Therefore we're done now.
                LoggingUtil.logFine(logger, "Ignoring snapshot %d phase 2 for %s: execution completed",
                        snapshotId, jobNameAndExecutionId());
                return CompletableFuture.completedFuture(null);
            }
            return snapshotContext.startNewSnapshotPhase2(snapshotId, success);
        }
    }

    public void handlePacket(int vertexId, int ordinal, Address sender, byte[] payload) {
        receiverQueuesMap.computeIfAbsent(new SenderReceiverKey(vertexId, ordinal, sender), CREATE_RECEIVER_QUEUE_FN)
                   .add(payload);
    }

    public boolean hasParticipant(Address member) {
        // once participants is not null, it's always not null
        return participants != null && participants.contains(member);
    }

    public long jobId() {
        return jobId;
    }

    public long executionId() {
        return executionId;
    }

    public String jobNameAndExecutionId() {
        return Util.jobNameAndExecutionId(jobName, executionId);
    }

    public boolean isLightJob() {
        return isLightJob;
    }

    public Address coordinator() {
        return coordinator;
    }

    public Map<SenderReceiverKey, SenderTasklet> senderMap() {
        return senderMap;
    }

    public Map<SenderReceiverKey, ReceiverTasklet> receiverMap() {
        return receiverMap;
    }

    @Nullable
    public String jobName() {
        return jobName;
    }

    public RawJobMetrics getJobMetrics() {
        return jobMetrics;
    }

    public void setJobMetrics(RawJobMetrics jobMetrics) {
        this.jobMetrics = jobMetrics;
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        if (!metricsEnabled) {
            return;
        }
        descriptor = descriptor.withTag(MetricTags.JOB, idToString(jobId))
                               .withTag(MetricTags.EXECUTION, idToString(executionId));

        context.collect(descriptor, EXECUTION_START_TIME, ProbeLevel.INFO, ProbeUnit.MS, startTime.get());
        context.collect(descriptor, EXECUTION_COMPLETION_TIME, ProbeLevel.INFO, ProbeUnit.MS, completionTime.get());

        for (Tasklet tasklet : tasklets) {
            tasklet.provideDynamicMetrics(descriptor.copy(), context);
        }
    }

    public void setCompletionTime() {
        completionTime.set(System.currentTimeMillis());
    }

    public CompletableFuture<Void> getExecutionFuture() {
        return executionFuture;
    }

    public long getCreatedOn() {
        return createdOn;
    }

    public static final class SenderReceiverKey {
        public final int vertexId;
        public final int ordinal;
        public final Address address;

        public SenderReceiverKey(int vertexId, int ordinal, @Nonnull Address address) {
            this.vertexId = vertexId;
            this.ordinal = ordinal;
            this.address = address;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SenderReceiverKey that = (SenderReceiverKey) o;
            return vertexId == that.vertexId && ordinal == that.ordinal && address.equals(that.address);
        }

        @Override
        public int hashCode() {
            return Objects.hash(vertexId, ordinal, address);
        }
    }
}
