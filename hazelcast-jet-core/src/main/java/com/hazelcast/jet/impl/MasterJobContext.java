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

package com.hazelcast.jet.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LocalMemberResetException;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.TerminationMode.ActionAfterTerminate;
import com.hazelcast.jet.impl.exception.JobTerminateRequestedException;
import com.hazelcast.jet.impl.exception.TerminatedWithSnapshotException;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.operation.CompleteExecutionOperation;
import com.hazelcast.jet.impl.operation.InitExecutionOperation;
import com.hazelcast.jet.impl.operation.StartExecutionOperation;
import com.hazelcast.jet.impl.operation.TerminateExecutionOperation;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.jet.impl.util.NonCompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.Operation;

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
import java.util.concurrent.atomic.AtomicBoolean;
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
import static com.hazelcast.jet.core.JobStatus.SUSPENDED_EXPORTING_SNAPSHOT;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.function.Functions.entryKey;
import static com.hazelcast.jet.impl.JobRepository.EXPORTED_SNAPSHOTS_PREFIX;
import static com.hazelcast.jet.impl.TerminationMode.ActionAfterTerminate.RESTART;
import static com.hazelcast.jet.impl.TerminationMode.ActionAfterTerminate.SUSPEND;
import static com.hazelcast.jet.impl.TerminationMode.CANCEL_FORCEFUL;
import static com.hazelcast.jet.impl.TerminationMode.CANCEL_GRACEFUL;
import static com.hazelcast.jet.impl.TerminationMode.RESTART_GRACEFUL;
import static com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject.deserializeWithCustomClassLoader;
import static com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder.createExecutionPlans;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isRestartableException;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isTopologyException;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

/**
 * Part of {@link MasterContext} that deals with execution starting and
 * termination.
 */
public class MasterJobContext {

    public static final int SNAPSHOT_RESTORE_EDGE_PRIORITY = Integer.MIN_VALUE;
    public static final String SNAPSHOT_VERTEX_PREFIX = "__snapshot_";

    private final MasterContext mc;
    private final ILogger logger;

    private volatile long executionStartTime;
    private volatile ExecutionInvocationCallback executionInvocationCallback;
    private volatile Set<Vertex> vertices;

    /**
     * A future (re)created when the job is started and completed when its
     * execution ends. Execution ending doesn't mean the job is done, it may
     * be just temporarily stopping due to suspension, job restarting, etc.
     *
     * <p>It's always completed normally, even if the execution fails.
     */
    @Nonnull
    private volatile CompletableFuture<Void> executionCompletionFuture = completedFuture(null);

    /**
     * A future completed when the job fully completes. It's NOT completed when
     * the job is suspended or when it is going to be restarted. It's used for
     * {@link Job#join()}.
     */
    private final NonCompletableFuture jobCompletionFuture = new NonCompletableFuture();

    /**
     * Null initially. When a job termination is requested, it is assigned a
     * termination mode. It's reset back to null when execute operations
     * complete.
     */
    private volatile TerminationMode requestedTerminationMode;

    MasterJobContext(MasterContext masterContext, ILogger logger) {
        this.mc = masterContext;
        this.logger = logger;
    }

    public CompletableFuture<Void> jobCompletionFuture() {
        return jobCompletionFuture;
    }

    TerminationMode requestedTerminationMode() {
        return requestedTerminationMode;
    }

    private boolean isCancelled() {
        return requestedTerminationMode == CANCEL_FORCEFUL;
    }

    /**
     * Starts execution of the job if it is not already completed, cancelled or failed.
     * If the job is already cancelled, the job completion procedure is triggered.
     * If the job quorum is not satisfied, job restart is rescheduled.
     * If there was a membership change and the partition table is not completely
     * fixed yet, job restart is rescheduled.
     */
    @SuppressWarnings("checkstyle:ReturnCount")
    void tryStartJob(Function<Long, Long> executionIdSupplier) {
        ClassLoader classLoader = null;
        DAG dag = null;
        Throwable exception = null;
        String dotString = null;

        mc.lock();
        synchronized_block:
        try {
            if (isCancelled()) {
                logger.fine("Skipping init job '" + mc.jobName() + "': is already cancelled.");
                exception = new CancellationException();
                break synchronized_block;
            }
            if (mc.jobStatus() != NOT_RUNNING) {
                logger.fine("Not starting job '" + mc.jobName() + "': status is " + mc.jobStatus());
                return;
            }

            if (mc.jobExecutionRecord().isSuspended()) {
                mc.jobExecutionRecord().setSuspended(false);
                mc.writeJobExecutionRecord(false);
                mc.setJobStatus(NOT_RUNNING);
            }

            if (scheduleRestartIfQuorumAbsent() || scheduleRestartIfClusterIsNotSafe()) {
                return;
            }
            executionStartTime = System.nanoTime();
            mc.setJobStatus(STARTING);

            // ensure JobExecutionRecord exists
            mc.writeJobExecutionRecord(true);

            if (requestedTerminationMode != null) {
                if (requestedTerminationMode.actionAfterTerminate() == RESTART) {
                    // ignore restart, we are just starting
                    requestedTerminationMode = null;
                } else {
                    exception = new JobTerminateRequestedException(requestedTerminationMode);
                    break synchronized_block;
                }
            }

            classLoader = mc.getJetService().getClassLoader(mc.jobId());
            try {
                dag = deserializeWithCustomClassLoader(mc.nodeEngine().getSerializationService(), classLoader,
                        mc.jobRecord().getDag());
            } catch (Exception e) {
                logger.warning("DAG deserialization failed", e);
                exception = e;
                break synchronized_block;
            }
            // save a copy of the vertex list because it is going to change
            vertices = new HashSet<>();
            dotString = dag.toDotString();
            dag.iterator().forEachRemaining(vertices::add);
            mc.setExecutionId(executionIdSupplier.apply(mc.jobId()));

            mc.snapshotContext().onExecutionStarted();
            executionCompletionFuture = new CompletableFuture<>();
        } finally {
            mc.unlock();
        }

        if (exception != null) {
            // run the finalizeJob outside of the synchronized block
            finalizeJob(exception);
            return;
        }

        // find snapshot to restore
        long snapshotToRestore = mc.jobExecutionRecord().snapshotId();
        try {
            mc.jobRepository().clearSnapshotData(mc.jobId(), mc.jobExecutionRecord().ongoingDataMapIndex());
        } catch (Exception e) {
            logger.warning("Cannot delete old snapshots for " + mc.jobName(), e);
        }
        String mapName = null;
        if (snapshotToRestore >= 0) {
            mapName = mc.jobExecutionRecord().successfulSnapshotDataMapName(mc.jobId());
        } else if (mc.jobConfig().getInitialSnapshotName() != null) {
            mapName = EXPORTED_SNAPSHOTS_PREFIX + mc.jobConfig().getInitialSnapshotName();
        }
        if (mapName != null) {
            try {
                rewriteDagWithSnapshotRestore(dag, snapshotToRestore, mapName);
            } catch (Exception e) {
                finalizeJob(e);
                return;
            }
        } else {
            logger.info("No previous snapshot for " + mc.jobIdString() + " found.");
        }

        MembersView membersView = getMembersView();
        ClassLoader previousCL = swapContextClassLoader(classLoader);
        try {
            logger.info("Start executing " + mc.jobIdString()
                    + ", execution graph in DOT format:\n" + dotString
                    + "\nHINT: You can use graphviz or http://viz-js.com to visualize the printed graph.");
            logger.fine("Building execution plan for " + mc.jobIdString());
            mc.setExecutionPlanMap(createExecutionPlans(mc.nodeEngine(), membersView, dag, mc.jobId(), mc.executionId(),
                    mc.jobConfig(), mc.jobExecutionRecord().ongoingSnapshotId()));
        } catch (Exception e) {
            finalizeJob(e);
            return;
        } finally {
            Thread.currentThread().setContextClassLoader(previousCL);
        }

        logger.fine("Built execution plans for " + mc.jobIdString());
        Set<MemberInfo> participants = mc.executionPlanMap().keySet();
        Function<ExecutionPlan, Operation> operationCtor = plan ->
                new InitExecutionOperation(mc.jobId(), mc.executionId(), membersView.getVersion(), participants,
                        mc.nodeEngine().getSerializationService().toData(plan));
        mc.invokeOnParticipants(operationCtor, this::onInitStepCompleted, null);
    }

    /**
     * Returns a tuple of:<ol>
     *     <li>a future that will be completed when the execution completes (or
     *          a completed future, if execution is not RUNNING or STARTING)
     *     <li>a string with a message why this call did nothing or null, if
     *          this call actually initiated the termination
     * </ol>
     *
     * @param allowWhileExportingSnapshot if false and jobStatus is
     *      SUSPENDED_EXPORTING_SNAPSHOT, termination will be rejected
     */
    @Nonnull
    Tuple2<CompletableFuture<Void>, String> requestTermination(TerminationMode mode, boolean allowWhileExportingSnapshot) {
        // Switch graceful method to forceful if we don't do snapshots, except for graceful
        // cancellation, which is allowed even if not snapshotting.
        if (mc.jobConfig().getProcessingGuarantee() == NONE && mode != CANCEL_GRACEFUL) {
            mode = mode.withoutTerminalSnapshot();
        }

        JobStatus localStatus;
        Tuple2<CompletableFuture<Void>, String> result;
        mc.lock();
        try {
            localStatus = mc.jobStatus();
            if (localStatus == SUSPENDED_EXPORTING_SNAPSHOT && !allowWhileExportingSnapshot) {
                return tuple2(executionCompletionFuture, "Cannot cancel when job status is "
                        + SUSPENDED_EXPORTING_SNAPSHOT);
            }
            if (localStatus == SUSPENDED && mode != CANCEL_FORCEFUL) {
                // if suspended, we can only cancel the job. Other terminations have no effect.
                return tuple2(executionCompletionFuture, "Job is " + SUSPENDED);
            }
            if (requestedTerminationMode != null) {
                // don't report cancellation of cancelled job as an error
                String message = requestedTerminationMode == CANCEL_FORCEFUL && mode == CANCEL_FORCEFUL ? null
                        : "Job is already terminating in mode: " + requestedTerminationMode.name();
                return tuple2(executionCompletionFuture, message);
            }
            requestedTerminationMode = mode;
            // handle cancellation of a suspended job
            if (localStatus == SUSPENDED || localStatus == SUSPENDED_EXPORTING_SNAPSHOT) {
                mc.setJobStatus(FAILED);
                setFinalResult(new CancellationException());
            }
            if (mode.isWithTerminalSnapshot()) {
                mc.snapshotContext().enqueueSnapshot(null, true, null);
            }

            result = tuple2(executionCompletionFuture, null);
        } finally {
            mc.unlock();
        }

        if (localStatus == SUSPENDED) {
            mc.coordinationService().completeJob(mc, System.currentTimeMillis(), new CancellationException());
        } else {
            if (localStatus == RUNNING || localStatus == STARTING) {
                handleTermination(mode);
            }
        }

        return result;
    }

    private void rewriteDagWithSnapshotRestore(DAG dag, long snapshotId, String mapName) {
        IMap<Object, Object> snapshotMap = mc.nodeEngine().getHazelcastInstance().getMap(mapName);
        snapshotId = SnapshotValidator.validateSnapshot(snapshotId, mc.jobIdString(), snapshotMap);
        logger.info("State of " + mc.jobIdString() + " will be restored from snapshot " + snapshotId + ", map=" + mapName);

        List<Vertex> originalVertices = new ArrayList<>();
        dag.iterator().forEachRemaining(originalVertices::add);

        Map<String, Integer> vertexToOrdinal = new HashMap<>();
        Vertex readSnapshotVertex = dag.newVertex(SNAPSHOT_VERTEX_PREFIX + "read",
                readMapP(mapName));
        long finalSnapshotId = snapshotId;
        Vertex explodeVertex = dag.newVertex(SNAPSHOT_VERTEX_PREFIX + "explode",
                () -> new ExplodeSnapshotP(vertexToOrdinal, finalSnapshotId));
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

    private boolean scheduleRestartIfQuorumAbsent() {
        int quorumSize = mc.jobExecutionRecord().getQuorumSize();
        if (mc.coordinationService().isQuorumPresent(quorumSize)) {
            return false;
        }

        logger.fine("Rescheduling restart of '" + mc.jobName() + "': quorum size " + quorumSize + " is not met");
        scheduleRestart();
        return true;
    }

    private boolean scheduleRestartIfClusterIsNotSafe() {
        if (mc.coordinationService().shouldStartJobs()) {
            return false;
        }

        logger.fine("Rescheduling restart of '" + mc.jobName() + "': cluster is not safe");
        scheduleRestart();
        return true;
    }

    private void scheduleRestart() {
        mc.assertLockHeld();
        JobStatus jobStatus = mc.jobStatus();
        if (jobStatus != NOT_RUNNING && jobStatus != STARTING && jobStatus != RUNNING) {
            throw new IllegalStateException("Restart scheduled in an unexpected state: " + jobStatus);
        }
        mc.setJobStatus(NOT_RUNNING);
        mc.coordinationService().scheduleRestart(mc.jobId());
    }

    private MembersView getMembersView() {
        ClusterServiceImpl clusterService = (ClusterServiceImpl) mc.nodeEngine().getClusterService();
        return clusterService.getMembershipManager().getMembersView();
    }

    // Called as callback when all InitOperation invocations are done
    private void onInitStepCompleted(Map<MemberInfo, Object> responses) {
        Throwable error = getResult("Init", responses);

        if (error == null) {
            JobStatus status = mc.jobStatus();

            if (status != STARTING) {
                error = new IllegalStateException("Cannot execute " + mc.jobIdString() + ": status is " + status);
            }
        }

        if (error == null) {
            invokeStartExecution();
        } else {
            invokeCompleteExecution(error);
        }
    }

    // If a participant leaves or the execution fails in a participant locally, executions are cancelled
    // on the remaining participants and the callback is completed after all invocations return.
    private void invokeStartExecution() {
        logger.fine("Executing " + mc.jobIdString());

        long executionId = mc.executionId();

        executionInvocationCallback = new ExecutionInvocationCallback(executionId);
        if (requestedTerminationMode != null) {
            handleTermination(requestedTerminationMode);
        }

        Function<ExecutionPlan, Operation> operationCtor = plan -> new StartExecutionOperation(mc.jobId(), executionId);
        Consumer<Map<MemberInfo, Object>> completionCallback = this::onExecuteStepCompleted;

        mc.setJobStatus(RUNNING);

        mc.invokeOnParticipants(operationCtor, completionCallback, executionInvocationCallback);

        if (mc.jobConfig().getProcessingGuarantee() != NONE) {
            mc.coordinationService().scheduleSnapshot(mc, executionId);
        }
    }

    private void handleTermination(@Nonnull TerminationMode mode) {
        // this method can be called multiple times to handle the termination, it must
        // be safe against it (idempotent).
        if (mode.isWithTerminalSnapshot()) {
            mc.snapshotContext().tryBeginSnapshot();
        } else if (executionInvocationCallback != null) {
            executionInvocationCallback.cancelInvocations(mode);
        }
    }

    // Called as callback when all ExecuteOperation invocations are done
    private void onExecuteStepCompleted(Map<MemberInfo, Object> responses) {
        invokeCompleteExecution(getResult("Execution", responses));
    }

    void setFinalResult(Throwable failure) {
        if (failure == null) {
            jobCompletionFuture.internalComplete();
        } else {
            jobCompletionFuture.internalCompleteExceptionally(failure);
        }
    }

    /**
     * <ul>
     * <li>Returns {@code null} if there is no failure
     * <li>Returns a {@link CancellationException} if the job is cancelled
     *     forcefully.
     * <li>Returns a {@link JobTerminateRequestedException} if the current
     *     execution is stopped due to a requested termination, except for
     *     CANCEL_GRACEFUL, in which case CancellationException is returned.
     * <li>If there is at least one user failure, such as an exception in user
     *     code (restartable or not), then returns that failure.
     * <li>Otherwise, the failure is because a job participant has left the
     *     cluster. In that case, it returns {@code TopologyChangeException} so
     *     that the job will be restarted
     * </ul>
     */
    private Throwable getResult(String opName, Map<MemberInfo, Object> responses) {
        if (isCancelled()) {
            logger.fine(mc.jobIdString() + " to be cancelled after " + opName);
            return new CancellationException();
        }

        Map<Boolean, List<Entry<MemberInfo, Object>>> grouped = groupResponses(responses);
        Collection<MemberInfo> successfulMembers = grouped.get(false).stream().map(Entry::getKey).collect(toList());

        List<Entry<MemberInfo, Object>> failures = grouped.get(true);
        if (!failures.isEmpty()) {
            logger.fine(opName + " of " + mc.jobIdString() + " has failures: " + failures);
        }

        if (successfulMembers.size() == mc.executionPlanMap().size()) {
            logger.fine(opName + " of " + mc.jobIdString() + " was successful");
            return null;
        }

        // Handle TerminatedWithSnapshotException. If only part of the members
        // threw it and others completed normally, the terminal snapshot will fail,
        // but we still handle it as if terminal snapshot was done. If there are
        // other exceptions, ignore this and handle the other exception.
        if (failures.stream().allMatch(entry -> entry.getValue() instanceof TerminatedWithSnapshotException)) {
            assert opName.equals("Execution") : "opName is '" + opName + "', expected 'Execution'";
            logger.fine(opName + " of " + mc.jobIdString() + " terminated after a terminal snapshot");
            TerminationMode mode = requestedTerminationMode;
            assert mode != null && mode.isWithTerminalSnapshot() : "mode=" + mode;
            return mode == CANCEL_GRACEFUL ? new CancellationException() : new JobTerminateRequestedException(mode);
        }

        // If there is no user-code exception, it means at least one job
        // participant has left the cluster. In that case, all remaining
        // participants return a TopologyChangedException.
        return failures
                .stream()
                .map(entry -> (Throwable) entry.getValue())
                .filter(e -> !(e instanceof CancellationException
                        || e instanceof TerminatedWithSnapshotException
                        || isTopologyException(e)))
                .findFirst()
                .map(ExceptionUtil::peel)
                .orElseGet(TopologyChangedException::new);
    }

    private void invokeCompleteExecution(Throwable error) {
        JobStatus status = mc.jobStatus();

        Throwable finalError;
        if (status == STARTING || status == RUNNING) {
            logger.fine("Sending CompleteExecutionOperation for " + mc.jobIdString());
            finalError = error;
        } else {
            logCannotComplete(error);
            finalError = new IllegalStateException("Job coordination failed");
        }

        Function<ExecutionPlan, Operation> operationCtor = plan ->
                new CompleteExecutionOperation(mc.executionId(), finalError);
        mc.invokeOnParticipants(operationCtor, responses -> onCompleteExecutionCompleted(error), null);
    }

    private void logCannotComplete(Throwable error) {
        if (error != null) {
            logger.severe("Cannot properly complete failed " + mc.jobIdString()
                    + ": status is " + mc.jobStatus(), error);
        } else {
            logger.severe("Cannot properly complete " + mc.jobIdString()
                    + ": status is " + mc.jobStatus());
        }
    }

    private void onCompleteExecutionCompleted(Throwable error) {
        if (error instanceof JobTerminateRequestedException
                && ((JobTerminateRequestedException) error).mode().isWithTerminalSnapshot()) {
            // have to use Async version, the future is completed inside a synchronized block
            mc.snapshotContext().terminalSnapshotFuture()
              .whenCompleteAsync(withTryCatch(logger, (r, e) -> finalizeJob(error)));
        } else {
            finalizeJob(error);
        }
    }

    private void cancelExecutionInvocations(long jobId, long executionId, TerminationMode mode) {
        mc.nodeEngine().getExecutionService().execute(ExecutionService.ASYNC_EXECUTOR, () ->
                mc.invokeOnParticipants(plan -> new TerminateExecutionOperation(jobId, executionId, mode), null, null));
    }

    // Called as callback when all CompleteOperation invocations are done
    void finalizeJob(@Nullable Throwable failure) {
        Runnable nonSynchronizedAction = () -> { };
        mc.lock();
        try {
            JobStatus status = mc.jobStatus();
            if (status == COMPLETED || status == FAILED) {
                logIgnoredCompletion(failure, status);
                return;
            }
            completeVertices(failure);

            boolean isSuccess = isSuccess(failure);

            // reset state for the next execution
            requestedTerminationMode = null;
            executionInvocationCallback = null;
            ActionAfterTerminate terminationModeAction = failure instanceof JobTerminateRequestedException
                    ? ((JobTerminateRequestedException) failure).mode().actionAfterTerminate() : null;
            mc.snapshotContext().onExecutionTerminated();

            // if restart was requested, restart immediately
            if (terminationModeAction == RESTART) {
                mc.setJobStatus(NOT_RUNNING);
                nonSynchronizedAction = () -> mc.coordinationService().restartJob(mc.jobId());
            } else if (isRestartableException(failure) && mc.jobConfig().isAutoScaling()) {
                // if restart is due to a failure, schedule a restart after a delay
                scheduleRestart();
            } else if (terminationModeAction == SUSPEND
                    || isRestartableException(failure)
                    && !mc.jobConfig().isAutoScaling()
                    && mc.jobConfig().getProcessingGuarantee() != NONE) {
                mc.setJobStatus(SUSPENDED);
                mc.jobExecutionRecord().setSuspended(true);
                nonSynchronizedAction = () -> mc.writeJobExecutionRecord(false);
            } else {
                mc.setJobStatus(isSuccess ? COMPLETED : FAILED);

                if (failure instanceof LocalMemberResetException) {
                    logger.fine("Cancelling job " + mc.jobIdString() + " locally: member (local or remote) reset. " +
                            "We don't delete job metadata: job will restart on majority cluster");
                    setFinalResult(new CancellationException());
                    return;
                }

                nonSynchronizedAction = () -> {
                    try {
                        mc.coordinationService().completeJob(mc, System.currentTimeMillis(), failure);
                    } catch (RuntimeException e) {
                        logger.warning("Completion of " + mc.jobIdString() + " failed", e);
                    } finally {
                        setFinalResult(failure);
                    }
                };
            }
        } finally {
            mc.unlock();
        }
        executionCompletionFuture.complete(null);
        nonSynchronizedAction.run();
    }

    private boolean isSuccess(@Nullable Throwable failure) {
        long elapsed = NANOSECONDS.toMillis(System.nanoTime() - executionStartTime);
        if (failure == null) {
            logger.info(String.format("Execution of %s completed in %,d ms", mc.jobIdString(), elapsed));
            return true;
        }
        if (failure instanceof CancellationException || failure instanceof JobTerminateRequestedException) {
            logger.info(String.format("Execution of %s completed in %,d ms, reason=%s",
                    mc.jobIdString(), elapsed, failure));
            return false;
        }
        logger.severe(String.format("Execution of %s failed after %,d ms", mc.jobIdString(), elapsed), failure);
        return false;
    }

    private void logIgnoredCompletion(@Nullable Throwable failure, JobStatus status) {
        if (failure != null) {
            logger.severe("Ignoring failure completion of " + idToString(mc.jobId()) + " because status is " + status,
                    failure);
        } else {
            logger.severe("Ignoring completion of " + idToString(mc.jobId()) + " because status is " + status);
        }
    }

    private void completeVertices(@Nullable Throwable failure) {
        if (vertices != null) {
            for (Vertex vertex : vertices) {
                try {
                    vertex.getMetaSupplier().close(failure);
                } catch (Exception e) {
                    logger.severe(mc.jobIdString()
                            + " encountered an exception in ProcessorMetaSupplier.complete(), ignoring it", e);
                }
            }
        }
    }

    private static ClassLoader swapContextClassLoader(ClassLoader jobClassLoader) {
        Thread currentThread = Thread.currentThread();
        ClassLoader previous = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(jobClassLoader);
        return previous;
    }

    void resumeJob(Function<Long, Long> executionIdSupplier) {
        mc.lock();
        try {
            if (mc.jobStatus() != SUSPENDED) {
                logger.info("Not resuming " + mc.jobIdString() + ": not " + SUSPENDED + ", but " + mc.jobStatus());
                return;
            }
            mc.setJobStatus(NOT_RUNNING);
        } finally {
            mc.unlock();
        }
        logger.fine("Resuming job " + mc.jobName());
        tryStartJob(executionIdSupplier);
    }

    private boolean hasParticipant(String uuid) {
        // a member is a participant when it is a master member (that's we) or it's in the execution plan
        Map<MemberInfo, ExecutionPlan> planMap = mc.executionPlanMap();
        return mc.nodeEngine().getLocalMember().getUuid().equals(uuid)
                || planMap != null && planMap.keySet().stream().anyMatch(mi -> mi.getUuid().equals(uuid));
    }

    /**
     * Called when job participant is going to gracefully shut down. Will
     * initiate terminal snapshot and when it's done, it will complete the
     * returned future.
     *
     * @return a future to wait for, which may be already completed
     */
    @Nonnull
    CompletableFuture<Void> onParticipantGracefulShutdown(String uuid) {
        return hasParticipant(uuid) ? gracefullyTerminate() : completedFuture(null);
    }

    @Nonnull
    CompletableFuture<Void> gracefullyTerminate() {
        return requestTermination(RESTART_GRACEFUL, false).f0();
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
    boolean maybeScaleUp(int dataMembersWithPartitionsCount) {
        if (!mc.jobConfig().isAutoScaling()) {
            return true;
        }

        // We only compare the number of our participating members and current members.
        // If there is any member in our participants that is not among current data members,
        // this job will be restarted anyway. If it's the other way, then the sizes won't match.
        if (mc.executionPlanMap() == null || mc.executionPlanMap().size() == dataMembersWithPartitionsCount) {
            LoggingUtil.logFine(logger, "Not scaling up %s: not running or already running on all members",
                    mc.jobIdString());
            return true;
        }

        JobStatus localStatus = mc.jobStatus();
        if (localStatus == RUNNING && requestTermination(TerminationMode.RESTART_GRACEFUL, false).f1() == null) {
            logger.info("Requested restart of " + mc.jobIdString() + " to make use of added member(s). "
                    + "Job was running on " + mc.executionPlanMap().size() + " members, cluster now has "
                    + dataMembersWithPartitionsCount + " data members with assigned partitions");
            return true;
        }

        // if status was not RUNNING or requestTermination didn't succeed, we'll try again later.
        return false;
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
                cancelExecutionInvocations(mc.jobId(), executionId, mode);
            }
        }
    }
}
