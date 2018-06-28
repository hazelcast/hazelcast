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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.operation.SnapshotOperation.SnapshotOperationResult;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static com.hazelcast.jet.Util.idToString;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * Data pertaining to single job execution on all cluster members. There's one
 * instance per job execution; if the job is restarted, another instance will
 * be used.
 */
public class ExecutionContext {

    private final long jobId;
    private final long executionId;
    private final Address coordinator;
    private final Set<Address> participants;
    private final Object executionLock = new Object();
    private final ILogger logger;
    private String jobName;

    // dest vertex id --> dest ordinal --> sender addr --> receiver tasklet
    private Map<Integer, Map<Integer, Map<Address, ReceiverTasklet>>> receiverMap = emptyMap();

    // dest vertex id --> dest ordinal --> dest addr --> sender tasklet
    private Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap = emptyMap();

    private List<ProcessorSupplier> procSuppliers = emptyList();
    private List<Processor> processors = emptyList();

    private List<Tasklet> tasklets = emptyList();

    // future which is completed only after all tasklets are completed and contains execution result
    private volatile CompletableFuture<Void> executionFuture;

    // future which can only be used to cancel the local execution.
    private final CompletableFuture<Void> cancellationFuture = new CompletableFuture<>();

    private final NodeEngine nodeEngine;
    private final TaskletExecutionService execService;
    private SnapshotContext snapshotContext;

    public ExecutionContext(NodeEngine nodeEngine, TaskletExecutionService execService,
                            long jobId, long executionId, Address coordinator, Set<Address> participants) {
        this.jobId = jobId;
        this.executionId = executionId;
        this.coordinator = coordinator;
        this.participants = new HashSet<>(participants);
        this.execService = execService;
        this.nodeEngine = nodeEngine;

        logger = nodeEngine.getLogger(getClass());
    }

    public ExecutionContext initialize(ExecutionPlan plan) {
        jobName = plan.getJobConfig().getName();
        if (jobName == null) {
            jobName = idToString(jobId);
        }
        // Must be populated early, so all processor suppliers are
        // available to be completed in the case of init failure
        procSuppliers = unmodifiableList(plan.getProcessorSuppliers());
        processors = plan.getProcessors();
        snapshotContext = new SnapshotContext(nodeEngine.getLogger(SnapshotContext.class), jobNameAndExecutionId(),
                plan.lastSnapshotId(), plan.getJobConfig().getProcessingGuarantee());
        plan.initialize(nodeEngine, jobId, executionId, snapshotContext);
        snapshotContext.initTaskletCount(plan.getStoreSnapshotTaskletCount(), plan.getHigherPriorityVertexCount());
        receiverMap = unmodifiableMap(plan.getReceiverMap());
        senderMap = unmodifiableMap(plan.getSenderMap());
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
     * instead {@link #cancelExecution()} should be used.
     */
    public CompletableFuture<Void> beginExecution() {
        synchronized (executionLock) {
            if (executionFuture != null) {
                // beginExecution was already called or execution was cancelled before it started.
                return executionFuture;
            } else {
                // begin job execution
                JetService service = nodeEngine.getService(JetService.SERVICE_NAME);
                ClassLoader cl = service.getClassLoader(jobId);
                executionFuture = execService.beginExecute(tasklets, cancellationFuture, cl);
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

        for (Tasklet tasklet : tasklets) {
            try {
                tasklet.close();
            } catch (Throwable e) {
                logger.severe(jobNameAndExecutionId()
                        + " encountered an exception in Processor.close(), ignoring it", e);
            }
        }

        for (ProcessorSupplier s : procSuppliers) {
            try {
                s.close(error);
            } catch (Throwable e) {
                logger.severe(jobNameAndExecutionId()
                        + " encountered an exception in ProcessorSupplier.complete(), ignoring it", e);
            }
        }
        MetricsRegistry metricsRegistry = ((NodeEngineImpl) nodeEngine).getMetricsRegistry();
        processors.forEach(metricsRegistry::deregister);
        tasklets.forEach(metricsRegistry::deregister);
    }

    /**
     * Cancels local execution of tasklets and returns a future which is only completed
     * when all tasklets are completed and contains the result of the execution.
     */
    public CompletableFuture<Void> cancelExecution() {
        synchronized (executionLock) {
            cancellationFuture.cancel(true);
            if (executionFuture == null) {
                // if cancelled before execution started, then assign the already completed future.
                executionFuture = cancellationFuture;
            }
            return executionFuture;
        }
    }

    /**
     * Starts a new snapshot by incrementing the current snapshot id
     */
    public CompletionStage<SnapshotOperationResult> beginSnapshot(long snapshotId) {
        synchronized (executionLock) {
            if (cancellationFuture.isDone() || executionFuture != null && executionFuture.isDone()) {
                throw new CancellationException();
            }
            return snapshotContext.startNewSnapshot(snapshotId);
        }
    }

    public void handlePacket(int vertexId, int ordinal, Address sender, BufferObjectDataInput in) {
        receiverMap.get(vertexId)
                   .get(ordinal)
                   .get(sender)
                   .receiveStreamPacket(in);
    }

    public boolean hasParticipant(Address member) {
        return participants.contains(member);
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

    public Address coordinator() {
        return coordinator;
    }

    public Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> senderMap() {
        return senderMap;
    }

    public Map<Integer, Map<Integer, Map<Address, ReceiverTasklet>>> receiverMap() {
        return receiverMap;
    }

    // visible for testing only
    public SnapshotContext snapshotContext() {
        return snapshotContext;
    }

    @Nullable
    public String jobName() {
        return jobName;
    }
}
