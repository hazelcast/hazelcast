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

package com.hazelcast.jet.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.LocalMemberResetException;
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.TerminationMode.ActionAfterTerminate;
import com.hazelcast.jet.impl.exception.JobTerminateRequestedException;
import com.hazelcast.jet.impl.exception.ShutdownInProgressException;
import com.hazelcast.jet.impl.exception.TerminatedWithSnapshotException;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.operation.CompleteExecutionOperation;
import com.hazelcast.jet.impl.operation.InitExecutionOperation;
import com.hazelcast.jet.impl.operation.SnapshotOperation;
import com.hazelcast.jet.impl.operation.SnapshotOperation.SnapshotOperationResult;
import com.hazelcast.jet.impl.operation.StartExecutionOperation;
import com.hazelcast.jet.impl.operation.TerminateExecutionOperation;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.jet.impl.util.NonCompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.NOT_RUNNING;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.impl.TerminationMode.ActionAfterTerminate.RESTART;
import static com.hazelcast.jet.impl.TerminationMode.ActionAfterTerminate.SUSPEND;
import static com.hazelcast.jet.impl.TerminationMode.CANCEL;
import static com.hazelcast.jet.impl.TerminationMode.RESTART_GRACEFUL;
import static com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject.deserializeWithCustomClassLoader;
import static com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder.createExecutionPlans;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isRestartableException;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isTopologyException;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.callbackOf;
import static com.hazelcast.jet.impl.util.Util.jobNameAndExecutionId;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

/**
 * Data pertaining to single job on master member. There's one instance per job,
 * shared between multiple executions.
 */
public class MasterContext {

    public static final int SNAPSHOT_RESTORE_EDGE_PRIORITY = Integer.MIN_VALUE;
    public static final String SNAPSHOT_VERTEX_PREFIX = "__snapshot_";

    private static final Object NULL_OBJECT = new Object() {
        @Override
        public String toString() {
            return "NULL_OBJECT";
        }
    };

    private final Object lock = new Object();

    private final NodeEngineImpl nodeEngine;
    private final JobCoordinationService coordinationService;
    private final ILogger logger;
    private final long jobId;
    private final String jobName;
    private final JobRepository jobRepository;
    private final JobRecord jobRecord;
    private final JobExecutionRecord jobExecutionRecord;
    private volatile JobStatus jobStatus = NOT_RUNNING;
    private volatile Set<Vertex> vertices;

    private volatile long executionId;
    private volatile long executionStartTime;
    private volatile Map<MemberInfo, ExecutionPlan> executionPlanMap;
    private volatile ExecutionInvocationCallback executionInvocationCallback;

    /**
     * A future completed when the job fully completes. It's NOT completed when
     * the job is suspended or when it is going to be restarted. It's used for
     * {@link Job#join()}.
     */
    private final NonCompletableFuture completionFuture = new NonCompletableFuture();

    /**
     * Null initially. When a job termination is requested, it is assigned a
     * termination mode. It's reset back to null when execute operations
     * complete.
     */
    private volatile TerminationMode requestedTerminationMode;

    /**
     * It's true while a snapshot is in progress. It's used to prevent
     * concurrent snapshots.
     */
    private boolean snapshotInProgress;

    /**
     * If {@code true}, the snapshot that will be executed next will be
     * terminal (a graceful shutdown). It is set to true when a graceful
     * shutdown or restart is requested and reset back to false when such a
     * snapshot is initiated.
     *
     * <p>If it's true at snapshot completion time, the next snapshot is not
     * scheduled after a delay but run immediately.
     */
    private volatile boolean nextSnapshotIsTerminal;

    /**
     * A future (re)created when the job is started and completed when terminal
     * snapshot is completed (successfully or not).
     */
    private CompletableFuture<Void> terminalSnapshotFuture;

    MasterContext(NodeEngineImpl nodeEngine, JobCoordinationService coordinationService, @Nonnull JobRecord jobRecord,
                  @Nonnull JobExecutionRecord jobExecutionRecord) {
        this.nodeEngine = nodeEngine;
        this.coordinationService = coordinationService;
        this.jobRepository = coordinationService.jobRepository();
        this.logger = nodeEngine.getLogger(getClass());
        this.jobRecord = jobRecord;
        this.jobExecutionRecord = jobExecutionRecord;
        this.jobId = jobRecord.getJobId();
        this.jobName = jobRecord.getJobNameOrId();
        if (jobExecutionRecord.isSuspended()) {
            jobStatus = SUSPENDED;
        }
    }

    public long jobId() {
        return jobId;
    }

    public long executionId() {
        return executionId;
    }

    public JobStatus jobStatus() {
        return jobStatus;
    }

    public JobConfig jobConfig() {
        return jobRecord.getConfig();
    }

    public JobRecord jobRecord() {
        return jobRecord;
    }

    public JobExecutionRecord jobExecutionRecord() {
        return jobExecutionRecord;
    }

    public CompletableFuture<Void> completionFuture() {
        return completionFuture;
    }

    /**
     * @return false, if termination was already requested
     */
    boolean requestTermination(TerminationMode mode) {
        JobStatus localStatus;
        assertLockNotHeld();
        synchronized (lock) {
            if (!isSnapshottingEnabled()) {
                // switch graceful method to forceful if we don't do snapshots
                mode = mode.withoutTerminalSnapshot();
            }

            localStatus = jobStatus();
            if (localStatus == SUSPENDED && mode != CANCEL) {
                // if suspended, we can only cancel the job. Other terminations have no effect.
                return false;
            }
            if (requestedTerminationMode != null) {
                return false;
            }
            requestedTerminationMode = mode;
            // handle cancellation of a suspended job
            if (localStatus == SUSPENDED) {
                this.jobStatus = COMPLETED;
                setFinalResult(new CancellationException());
            }
        }

        if (localStatus == SUSPENDED) {
            coordinationService.completeJob(this, System.currentTimeMillis(), new CancellationException());
        } else {
            if (localStatus == RUNNING || localStatus == STARTING) {
                handleTermination(mode);
            }
        }

        return true;
    }

    boolean isCancelled() {
        return requestedTerminationMode == CANCEL;
    }

    TerminationMode requestedTerminationMode() {
        return requestedTerminationMode;
    }

    /**
     * Starts execution of the job if it is not already completed, cancelled or failed.
     * If the job is already cancelled, the job completion procedure is triggered.
     * If the job quorum is not satisfied, job restart is rescheduled.
     * If there was a membership change and the partition table is not completely
     * fixed yet, job restart is rescheduled.
     */
    void tryStartJob(Function<Long, Long> executionIdSupplier) {
        ClassLoader classLoader = null;
        DAG dag = null;
        Throwable exception = null;
        String dotString = null;

        assertLockNotHeld();
        synchronized_block:
        synchronized (lock) {
            if (isCancelled()) {
                logger.fine("Skipping init job '" + jobName + "': is already cancelled.");
                exception = new CancellationException();
                break synchronized_block;
            }

            if (!setJobStatusToStarting()
                    || scheduleRestartIfQuorumAbsent()
                    || scheduleRestartIfClusterIsNotSafe()) {
                return;
            }

            // ensure JobExecutionRecord exists
            writeJobExecutionRecord(true);

            if (requestedTerminationMode != null) {
                if (requestedTerminationMode.actionAfterTerminate() == RESTART) {
                    // ignore restart, we are just starting
                    requestedTerminationMode = null;
                } else {
                    exception = new JobTerminateRequestedException(requestedTerminationMode);
                    break synchronized_block;
                }
            }

            classLoader = coordinationService.getJetService().getClassLoader(jobId);
            try {
                dag = deserializeWithCustomClassLoader(nodeEngine.getSerializationService(), classLoader,
                        jobRecord.getDag());
            } catch (Exception e) {
                logger.warning("DAG deserialization failed", e);
                exception = e;
                break synchronized_block;
            }
            // save a copy of the vertex list because it is going to change
            vertices = new HashSet<>();
            dotString = dag.toDotString();
            dag.iterator().forEachRemaining(vertices::add);
            executionId = executionIdSupplier.apply(jobId);

            snapshotInProgress = false;
            nextSnapshotIsTerminal = false;
            terminalSnapshotFuture = new CompletableFuture<>();
        }

        if (exception != null) {
            // run the finalizeJob outside of the synchronized block
            finalizeJob(exception);
            return;
        }

        if (isSnapshottingEnabled()) {
            long snapshotToRestore = jobExecutionRecord.snapshotId();
            try {
                jobRepository.clearSnapshotData(jobId, jobExecutionRecord.ongoingDataMapIndex());
            } catch (Exception e) {
                logger.warning("Cannot delete old snapshots for " + jobName, e);
            }
            if (snapshotToRestore >= 0) {
                rewriteDagWithSnapshotRestore(dag, snapshotToRestore);
            } else {
                logger.info("No previous snapshot for " + jobIdString() + " found.");
            }
        }

        MembersView membersView = getMembersView();
        ClassLoader previousCL = swapContextClassLoader(classLoader);
        try {
            logger.info("Start executing " + jobIdString()
                    + ", execution graph in DOT format:\n" + dotString
                    + "\nHINT: You can use graphviz or http://viz-js.com to visualize the printed graph.");
            logger.fine("Building execution plan for " + jobIdString());
            executionPlanMap = createExecutionPlans(nodeEngine, membersView, dag, jobId, executionId,
                    jobConfig(), jobExecutionRecord.ongoingSnapshotId());
        } catch (Exception e) {
            logger.severe("Exception creating execution plan for " + jobIdString(), e);
            finalizeJob(e);
            return;
        } finally {
            Thread.currentThread().setContextClassLoader(previousCL);
        }

        logger.fine("Built execution plans for " + jobIdString());
        Set<MemberInfo> participants = executionPlanMap.keySet();
        Function<ExecutionPlan, Operation> operationCtor = plan ->
                new InitExecutionOperation(jobId, executionId, membersView.getVersion(), participants,
                        nodeEngine.getSerializationService().toData(plan));
        invokeOnParticipants(operationCtor, this::onInitStepCompleted, null);
    }

    private void rewriteDagWithSnapshotRestore(DAG dag, long snapshotId) {
        String mapName = jobExecutionRecord.successfulSnapshotDataMapName(jobId);
        logger.info("State of " + jobIdString() + " will be restored from snapshot " + snapshotId + ", map=" + mapName);

        List<Vertex> originalVertices = new ArrayList<>();
        dag.iterator().forEachRemaining(originalVertices::add);

        Map<String, Integer> vertexToOrdinal = new HashMap<>();
        Vertex readSnapshotVertex = dag.newVertex(SNAPSHOT_VERTEX_PREFIX + "read",
                readMapP(mapName));
        Vertex explodeVertex = dag.newVertex(SNAPSHOT_VERTEX_PREFIX + "explode",
                () -> new ExplodeSnapshotP(vertexToOrdinal, snapshotId));
        dag.edge(between(readSnapshotVertex, explodeVertex).isolated());

        int index = 0;
        // add the edges
        for (Vertex userVertex : originalVertices) {
            vertexToOrdinal.put(userVertex.getName(), index);
            int destOrdinal = dag.getInboundEdges(userVertex.getName()).size();
            dag.edge(new SnapshotRestoreEdge(explodeVertex, index, userVertex, destOrdinal));
            index++;
        }
    }

    /**
     * Sets job status to starting.
     * Returns false if the job start process cannot proceed.
     */
    private boolean setJobStatusToStarting() {
        assertLockHeld();
        JobStatus status = jobStatus();
        if (status != NOT_RUNNING) {
            logger.fine("Not starting job '" + jobName + "': status is " + status);
            return false;
        }

        assert jobStatus == NOT_RUNNING : "cannot start job " + idToString(jobId) + " with status: " + jobStatus;
        jobStatus = STARTING;
        executionStartTime = System.nanoTime();
        if (jobExecutionRecord.isSuspended()) {
            jobExecutionRecord.setSuspended(false);
            writeJobExecutionRecord(false);
        }

        return true;
    }

    private boolean scheduleRestartIfQuorumAbsent() {
        int quorumSize = jobExecutionRecord.getQuorumSize();
        if (coordinationService.isQuorumPresent(quorumSize)) {
            return false;
        }

        logger.fine("Rescheduling restart of job '" + jobName + "': quorum size " + quorumSize + " is not met");
        scheduleRestart();
        return true;
    }

    private boolean scheduleRestartIfClusterIsNotSafe() {
        if (coordinationService.shouldStartJobs()) {
            return false;
        }

        logger.fine("Rescheduling restart of job '" + jobName + "': cluster is not safe");
        scheduleRestart();
        return true;
    }

    private void scheduleRestart() {
        assertLockHeld();
        if (jobStatus != NOT_RUNNING && jobStatus != STARTING && jobStatus != RUNNING) {
            throw new IllegalStateException("Restart scheduled in an unexpected state: " + jobStatus);
        }
        jobStatus = NOT_RUNNING;
        coordinationService.scheduleRestart(jobId);
    }

    private MembersView getMembersView() {
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
        return clusterService.getMembershipManager().getMembersView();
    }

    // Called as callback when all InitOperation invocations are done
    private void onInitStepCompleted(Map<MemberInfo, Object> responses) {
        Throwable error = getResult("Init", responses);

        if (error == null) {
            JobStatus status = jobStatus();

            if (status != STARTING) {
                error = new IllegalStateException("Cannot execute " + jobIdString() + ": status is " + status);
            }
        }

        if (error == null) {
            invokeStartExecution();
        } else {
            invokeCompleteExecution(error);
        }
    }

    // true -> failures, false -> success responses
    private Map<Boolean, List<Entry<MemberInfo, Object>>> groupResponses(Map<MemberInfo, Object> responses) {
        Map<Boolean, List<Entry<MemberInfo, Object>>> grouped = responses
                .entrySet()
                .stream()
                .collect(partitioningBy(e -> e.getValue() instanceof Throwable));

        grouped.putIfAbsent(true, emptyList());
        grouped.putIfAbsent(false, emptyList());

        return grouped;
    }

    // If a participant leaves or the execution fails in a participant locally, executions are cancelled
    // on the remaining participants and the callback is completed after all invocations return.
    private void invokeStartExecution() {
        logger.fine("Executing " + jobIdString());

        long executionId = this.executionId;

        executionInvocationCallback = new ExecutionInvocationCallback(executionId);
        if (requestedTerminationMode != null) {
            handleTermination(requestedTerminationMode);
        }

        Function<ExecutionPlan, Operation> operationCtor = plan -> new StartExecutionOperation(jobId, executionId);
        Consumer<Map<MemberInfo, Object>> completionCallback = this::onExecuteStepCompleted;

        jobStatus = RUNNING;

        invokeOnParticipants(operationCtor, completionCallback, executionInvocationCallback);

        if (isSnapshottingEnabled()) {
            coordinationService.scheduleSnapshot(jobId, executionId);
        }
    }

    private void handleTermination(@Nonnull TerminationMode mode) {
        // this method can be called multiple times to handle the termination, it must
        // be safe against it (idempotent).
        if (mode.isWithTerminalSnapshot()) {
            nextSnapshotIsTerminal = true;
            beginSnapshot(executionId);
        } else {
            if (executionInvocationCallback != null) {
                executionInvocationCallback.cancelInvocations(mode);
            }
        }
    }

    private void cancelExecutionInvocations(long jobId, long executionId, TerminationMode mode) {
        nodeEngine.getExecutionService().execute(ExecutionService.ASYNC_EXECUTOR, () ->
                invokeOnParticipants(plan -> new TerminateExecutionOperation(jobId, executionId, mode), null, null));
    }

    void beginSnapshot(long executionId) {
        boolean isTerminal;
        assertLockNotHeld();
        synchronized (lock) {
            if (this.executionId != executionId) {
                // Current execution is completed and probably a new execution has started, but we don't
                // cancel the scheduled snapshot from previous execution, so let's just ignore it.
                logger.fine("Not beginning snapshot since unexpected execution ID received for " + jobIdString()
                        + ". Received execution ID: " + idToString(executionId));
                return;
            }

            if (jobStatus != RUNNING) {
                logger.fine("Not beginning snapshot, job is not RUNNING, but " + jobStatus);
                return;
            }

            if (snapshotInProgress) {
                logger.fine("Not beginning snapshot since one is already in progress " + jobIdString());
                return;
            }
            if (terminalSnapshotFuture.isDone()) {
                logger.fine("Not beginning snapshot since terminal snapshot is already completed");
                return;
            }
            snapshotInProgress = true;
            isTerminal = nextSnapshotIsTerminal;
            jobExecutionRecord.startNewSnapshot();
        }

        writeJobExecutionRecord(false);
        long newSnapshotId = jobExecutionRecord.ongoingSnapshotId();

        logger.info(String.format("Starting%s snapshot %s for %s", isTerminal ? " terminal" : "", newSnapshotId,
                jobIdString()));
        Function<ExecutionPlan, Operation> factory =
                plan -> new SnapshotOperation(jobId, executionId, newSnapshotId,
                        jobExecutionRecord.ongoingDataMapIndex(), isTerminal);

        invokeOnParticipants(factory,
                responses -> onSnapshotCompleted(responses, executionId, newSnapshotId, isTerminal), null);
    }

    private void onSnapshotCompleted(Map<MemberInfo, Object> responses, long executionId, long snapshotId,
                                                  boolean wasTerminal) {
        // Note: this method can be called after finalizeJob() is called or even after new execution started.
        // We only wait for snapshot completion if the job completed with a terminal snapshot and the job
        // was successful.
        SnapshotOperationResult mergedResult = new SnapshotOperationResult();
        for (Object response : responses.values()) {
            // the response is either SnapshotOperationResult or an exception, see #invokeOnParticipants() method
            if (response instanceof Throwable) {
                response = new SnapshotOperationResult(0, 0, 0, (Throwable) response);
            }
            mergedResult.merge((SnapshotOperationResult) response);
        }

        boolean isSuccess = mergedResult.getError() == null;
        if (!isSuccess) {
            logger.warning(jobIdString() + " snapshot " + snapshotId + " failed on some member(s), " +
                    "one of the failures: " + mergedResult.getError());
        }
        jobExecutionRecord.ongoingSnapshotDone(
                mergedResult.getNumBytes(), mergedResult.getNumKeys(), mergedResult.getNumChunks(),
                mergedResult.getError());
        writeJobExecutionRecord(false);
        logger.info(String.format("Snapshot %d for %s completed with status %s in %dms, " +
                        "%,d bytes, %,d keys in %,d chunks, stored in data map %d",
                snapshotId, jobIdString(), isSuccess ? "SUCCESS" : "FAILURE",
                jobExecutionRecord.duration(), jobExecutionRecord.numBytes(),
                jobExecutionRecord.numKeys(), jobExecutionRecord.numChunks(),
                jobExecutionRecord.dataMapIndex()));
        jobRepository.clearSnapshotData(jobId, jobExecutionRecord.ongoingDataMapIndex());

        Runnable nonSynchronizedAction = () -> { };
        synchronized (lock) {
            if (this.executionId != executionId) {
                logger.fine("Not completing terminalSnapshotFuture on " + jobIdString() + ", new execution " +
                        "already started, snapshot was for executionId=" + idToString(executionId));
                return;
            }
            assert snapshotInProgress : "snapshot not in progress";
            snapshotInProgress = false;
            if (wasTerminal) {
                // after a terminal snapshot, no more snapshots are scheduled in this execution
                boolean completedNow = terminalSnapshotFuture.complete(null);
                assert completedNow : "terminalSnapshotFuture was already completed";
            } else {
                // schedule next snapshot after a delay or immediately, if it is terminal
                if (nextSnapshotIsTerminal) {
                    nonSynchronizedAction = () -> coordinationService.beginSnapshot(jobId, executionId);
                } else {
                    coordinationService.scheduleSnapshot(jobId, executionId);
                }
            }
        }
        nonSynchronizedAction.run();
    }

    // Called as callback when all ExecuteOperation invocations are done
    private void onExecuteStepCompleted(Map<MemberInfo, Object> responses) {
        invokeCompleteExecution(getResult("Execution", responses));
    }

    /**
     * <ul>
     * <li>Returns null if there is no failure.
     * <li>Returns a CancellationException if the job is cancelled.
     * <li>Returns a JobRestartRequestedException if the current execution is stopped to be restarted
     * <li>Returns a JobSuspendRequestedException if the current execution is stopped to be suspended
     * <li>If there is at least one user failure, such as an exception in user code (restartable or not), then
     *   returns that failure.
     * <li>Otherwise, the failure is because a job participant has left the cluster.
     *   In that case, {@code TopologyChangeException} is returned so that the job will be restarted.
     * </ul>
     */
    private Throwable getResult(String opName, Map<MemberInfo, Object> responses) {
        if (isCancelled()) {
            logger.fine(jobIdString() + " to be cancelled after " + opName);
            return new CancellationException();
        }

        Map<Boolean, List<Entry<MemberInfo, Object>>> grouped = groupResponses(responses);
        Collection<MemberInfo> successfulMembers = grouped.get(false).stream().map(Entry::getKey).collect(toList());

        List<Entry<MemberInfo, Object>> failures = grouped.get(true);
        if (!failures.isEmpty()) {
            logger.fine(opName + " of " + jobIdString() + " has failures: " + failures);
        }

        if (successfulMembers.size() == executionPlanMap.size()) {
            logger.fine(opName + " of " + jobIdString() + " was successful");
            return null;
        }

        // handle TerminatedWithSnapshotException
        // If only part of the members threw it and other completed normally, the terminal snapshot will fail, but
        // we still handle it as if terminal snapshot was done.
        // If there are other exceptions, ignore this and handle the other exception.
        if (failures.stream().allMatch(e -> e.getValue() instanceof TerminatedWithSnapshotException)) {
            assert opName.equals("Execution") : "opName=" + opName;
            logger.fine(opName + " of " + jobIdString() + " terminated after a terminal snapshot");
            TerminationMode mode = requestedTerminationMode;
            assert mode != null && mode.isWithTerminalSnapshot() : "mode=" + mode;
            return new JobTerminateRequestedException(mode);
        }

        // If there is no user-code exception, it means at least one job participant has left the cluster.
        // In that case, all remaining participants return a TopologyChangedException.
        return failures
                .stream()
                .peek(e -> {
                    if (e.getValue() instanceof ShutdownInProgressException) {
                        coordinationService.addShuttingDownMember(e.getKey().getUuid());
                    }
                })
                .map(e -> (Throwable) e.getValue())
                .filter(t -> !(t instanceof CancellationException) && !(t instanceof TerminatedWithSnapshotException))
                .filter(t -> !isTopologyException(t))
                .findFirst()
                .map(ExceptionUtil::peel)
                .orElseGet(TopologyChangedException::new);
    }

    private void invokeCompleteExecution(Throwable error) {
        JobStatus status = jobStatus();

        Throwable finalError;
        if (status == STARTING || status == RUNNING) {
            logger.fine("Completing " + jobIdString());
            finalError = error;
        } else {
            if (error != null) {
                logger.severe("Cannot properly complete failed " + jobIdString()
                        + ": status is " + status, error);
            } else {
                logger.severe("Cannot properly complete " + jobIdString()
                        + ": status is " + status);
            }

            finalError = new IllegalStateException("Job coordination failed.");
        }

        Function<ExecutionPlan, Operation> operationCtor = plan -> new CompleteExecutionOperation(executionId, finalError);
        invokeOnParticipants(operationCtor, responses -> onCompleteExecutionCompleted(error), null);
    }

    private void onCompleteExecutionCompleted(Throwable error) {
        if (error instanceof JobTerminateRequestedException
                && ((JobTerminateRequestedException) error).mode().isWithTerminalSnapshot()) {
            // have to use Async version, the future is completed inside a synchronized block
            terminalSnapshotFuture.whenCompleteAsync(withTryCatch(logger, (r, e) -> finalizeJob(error)));
        } else {
            finalizeJob(error);
        }
    }

    // Called as callback when all CompleteOperation invocations are done
    void finalizeJob(@Nullable Throwable failure) {
        Runnable nonSynchronizedAction = () -> { };
        assertLockNotHeld();
        synchronized (lock) {
            if (!checkJobNotDone(failure)) {
                return;
            }

            completeVertices(failure);

            long elapsed = NANOSECONDS.toMillis(System.nanoTime() - executionStartTime);
            boolean isSuccess = failure == null
                    || failure instanceof CancellationException
                    || failure instanceof JobTerminateRequestedException;
            if (isSuccess) {
                if (failure != null) {
                    logger.info(String.format("Execution of %s completed in %,d ms, reason=%s",
                            jobIdString(), elapsed, failure));
                } else {
                    logger.info(String.format("Execution of %s completed in %,d ms", jobIdString(), elapsed));
                }
            } else {
                logger.warning(String.format("Execution of %s failed after %,d ms", jobIdString(), elapsed), failure);
            }

            // reset state for the next execution
            requestedTerminationMode = null;
            executionInvocationCallback = null;
            ActionAfterTerminate terminationModeAction = failure instanceof JobTerminateRequestedException
                    ? ((JobTerminateRequestedException) failure).mode().actionAfterTerminate() : null;

            // if restart was requested, restart immediately
            if (terminationModeAction == RESTART) {
                jobStatus = NOT_RUNNING;
                nonSynchronizedAction = () -> coordinationService.restartJob(jobId);
            } else if (isRestartableException(failure) && jobRecord.getConfig().isAutoScaling()) {
                // if restart is due to a failure, schedule a restart after a delay
                scheduleRestart();
            } else if (terminationModeAction == SUSPEND
                    || isRestartableException(failure)
                            && !jobRecord.getConfig().isAutoScaling()
                            && jobRecord.getConfig().getProcessingGuarantee() != NONE) {
                jobStatus = SUSPENDED;
                jobExecutionRecord.setSuspended(true);
                nonSynchronizedAction = () -> writeJobExecutionRecord(false);
            } else {
                jobStatus = (isSuccess ? COMPLETED : FAILED);

                if (failure instanceof LocalMemberResetException) {
                    logger.fine("Cancelling job " + jobIdString() + " locally: member (local or remote) reset. " +
                            "We don't delete job metadata: job will restart on majority cluster");
                    setFinalResult(new CancellationException());
                    return;
                }

                nonSynchronizedAction = () -> {
                    try {
                        coordinationService.completeJob(this, System.currentTimeMillis(), failure);
                    } catch (RuntimeException e) {
                        logger.warning("Completion of " + jobIdString() + " failed", e);
                    } finally {
                        setFinalResult(failure);
                    }
                };
            }
        }
        nonSynchronizedAction.run();
    }

    /**
     * @return true, if job is not done
     */
    private boolean checkJobNotDone(@Nullable Throwable failure) {
        JobStatus status = jobStatus();
        if (status == COMPLETED || status == FAILED) {
            if (failure != null) {
                logger.severe("Ignoring failure completion of " + idToString(jobId) + " because status is "
                        + status, failure);
            } else {
                logger.severe("Ignoring completion of " + idToString(jobId) + " because status is " + status);
            }
            return false;
        }
        return true;
    }

    private void completeVertices(@Nullable Throwable failure) {
        if (vertices != null) {
            for (Vertex vertex : vertices) {
                try {
                    vertex.getMetaSupplier().close(failure);
                } catch (Exception e) {
                    logger.severe(jobIdString()
                            + " encountered an exception in ProcessorMetaSupplier.complete(), ignoring it", e);
                }
            }
        }
    }

    void setFinalResult(Throwable failure) {
        if (failure == null) {
            completionFuture.internalComplete();
        } else {
            completionFuture.internalCompleteExceptionally(failure);
        }
    }

    void updateQuorumSize(int newQuorumSize) {
        // This method can be called in parallel if multiple members are added. We don't synchronize here,
        // but the worst that can happen is that we write the JobRecord out unnecessarily.
        if (jobExecutionRecord.getQuorumSize() < newQuorumSize) {
            jobExecutionRecord.setLargerQuorumSize(newQuorumSize);
            writeJobExecutionRecord(false);
            logger.info("Current quorum size: " + jobExecutionRecord.getQuorumSize() + " of job "
                    + idToString(jobRecord.getJobId()) + " is updated to: " + newQuorumSize);
        }
    }

    private void writeJobExecutionRecord(boolean canCreate) {
        try {
            coordinationService.jobRepository().writeJobExecutionRecord(jobRecord.getJobId(), jobExecutionRecord,
                    canCreate);
        } catch (RuntimeException e) {
            // We don't bubble up the exceptions, if we can't write the record out, the universe is
            // probably crumbling apart anyway. And we don't depend on it, we only write out for
            // others to know or for the case should the master we fail.
            logger.warning("Failed to update JobRecord", e);
        }
    }

    /**
     * @param completionCallback a consumer that will receive a map of
     *                           responses, one for each member, after all have
     *                           been received. The value will be either the
     *                           response or an exception thrown from the
     *                           operation
     * @param callback A callback that will be called after each individual
     *                operation for each member completes
     */
    private void invokeOnParticipants(Function<ExecutionPlan, Operation> operationCtor,
                                      @Nullable Consumer<Map<MemberInfo, Object>> completionCallback,
                                      @Nullable ExecutionCallback<Object> callback) {
        ConcurrentMap<MemberInfo, Object> responses = new ConcurrentHashMap<>();
        AtomicInteger remainingCount = new AtomicInteger(executionPlanMap.size());
        for (Entry<MemberInfo, ExecutionPlan> entry : executionPlanMap.entrySet()) {
            MemberInfo member = entry.getKey();
            Operation op = operationCtor.apply(entry.getValue());
            InternalCompletableFuture<Object> future = nodeEngine.getOperationService()
                    .createInvocationBuilder(JetService.SERVICE_NAME, op, member.getAddress())
                    .invoke();

            if (completionCallback != null) {
                future.andThen(callbackOf((r, throwable) -> {
                    Object response = r != null ? r : throwable != null ? peel(throwable) : NULL_OBJECT;
                    Object oldResponse = responses.put(member, response);
                    assert oldResponse == null :
                            "Duplicate response for " + member + ". Old=" + oldResponse + ", new=" + response;
                    if (remainingCount.decrementAndGet() == 0) {
                        completionCallback.accept(responses);
                    }
                }));
            }

            if (callback != null) {
                future.andThen(callback);
            }
        }
    }

    private boolean isSnapshottingEnabled() {
        return jobConfig().getProcessingGuarantee() != NONE;
    }

    String jobName() {
        return jobName;
    }

    String jobIdString() {
        return jobNameAndExecutionId(jobName, executionId);
    }

    private static ClassLoader swapContextClassLoader(ClassLoader jobClassLoader) {
        Thread currentThread = Thread.currentThread();
        ClassLoader previous = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(jobClassLoader);
        return previous;
    }

    void resumeJob(Function<Long, Long> executionIdSupplier) {
        synchronized (lock) {
            if (jobStatus != SUSPENDED) {
                logger.info("Not resuming " + jobIdString() + ": not " + SUSPENDED + ", but " + jobStatus);
                return;
            }
            jobStatus = NOT_RUNNING;
        }
        logger.fine("Resuming job " + jobName);
        tryStartJob(executionIdSupplier);
    }

    private boolean hasParticipant(String uuid) {
        return executionPlanMap != null
                && executionPlanMap.keySet().stream().anyMatch(mi -> mi.getUuid().equals(uuid));
    }

    /**
     * Called when job participant is going to gracefully shut down. Will
     * initiate terminal snapshot and when it's done, will complete the
     * returned future.
     *
     * @return a future to wait for or null if there's no need to wait
     */
    @Nullable
    CompletableFuture<Void> onParticipantGracefulShutdown(String uuid) {
        if (!hasParticipant(uuid)) {
            return null;
        }

        if (jobStatus() == SUSPENDED) {
            return null;
        }

        requestTermination(RESTART_GRACEFUL);
        TerminationMode terminationMode = requestedTerminationMode;
        if (terminationMode != null && terminationMode.isWithTerminalSnapshot()) {
            // this future is null if job is not running, which is ok
            return terminalSnapshotFuture;
        }
        return null; // nothing to wait for
    }

    /**
     * Checks if the job is running on all members and maybe restart it.
     *
     * <p>Returns {@code false}, if this method should be scheduled to
     * be called later. That is, when the job is running, but we've
     * failed to request the restart.
     *
     * <p>Returns {@code true}, if the job is not running, has
     * auto-scaling disabled, is already running on all members or if
     * we've managed to request a restart.
     */
    boolean maybeScaleUp(Collection<Member> currentDataMembers) {
        if (!jobConfig().isAutoScaling()) {
            return true;
        }

        // We only compare the number of our participating members and current members.
        // If there is any member in our participants that is not among current data members,
        // this job will be restarted anyway. If it's the other way, then the sizes won't match.
        if (executionPlanMap == null || executionPlanMap.size() == currentDataMembers.size()) {
            LoggingUtil.logFine(logger, "Not scaling %s up: not running or already running on all members",
                    jobIdString());
            return true;
        }

        JobStatus localStatus = jobStatus;
        if (localStatus == RUNNING && requestTermination(TerminationMode.RESTART_GRACEFUL)) {
            logger.info("Requested restart of " + jobIdString() + " to make use of added member(s)");
            return true;
        }

        // if status was not RUNNING or requestTermination didn't succeed, we'll try again later.
        return false;
    }

    private void assertLockHeld() {
        assert Thread.holdsLock(lock) : "the lock should be held at this place";
    }

    private void assertLockNotHeld() {
        assert !Thread.holdsLock(lock) : "the lock should not be held at this place";
    }

    /**
     * Specific type of edge to be used when restoring snapshots
     */
    private static class SnapshotRestoreEdge extends Edge {

        SnapshotRestoreEdge(Vertex source, int sourceOrdinal, Vertex destination, int destOrdinal) {
            super(source, sourceOrdinal, destination, destOrdinal);
            distributed();
            partitioned(entryKey());
        }

        @Override
        public int getPriority() {
            return SNAPSHOT_RESTORE_EDGE_PRIORITY;
        }
    }

    /**
     * Registered to {@link StartExecutionOperation} invocations to cancel invocations in case of a failure or restart
     */
    private class ExecutionInvocationCallback implements ExecutionCallback<Object> {

        private final AtomicBoolean invocationsCancelled = new AtomicBoolean();
        private final long executionId;

        ExecutionInvocationCallback(long executionId) {
            this.executionId = executionId;
        }

        @Override
        public void onResponse(Object response) {

        }

        @Override
        public void onFailure(Throwable t) {
            if (!(peel(t) instanceof TerminatedWithSnapshotException)) {
                cancelInvocations(null);
            }
        }

        void cancelInvocations(TerminationMode mode) {
            if (invocationsCancelled.compareAndSet(false, true)) {
                cancelExecutionInvocations(jobId, executionId, mode);
            }
        }
    }
}
