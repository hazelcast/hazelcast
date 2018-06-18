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
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.exception.JobRestartRequestedException;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.operation.CancelExecutionOperation;
import com.hazelcast.jet.impl.operation.CompleteExecutionOperation;
import com.hazelcast.jet.impl.operation.InitExecutionOperation;
import com.hazelcast.jet.impl.operation.SnapshotOperation;
import com.hazelcast.jet.impl.operation.SnapshotOperation.SnapshotOperationResult;
import com.hazelcast.jet.impl.operation.StartExecutionOperation;
import com.hazelcast.jet.impl.util.CompletionToken;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.impl.util.NonCompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nullable;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.NOT_STARTED;
import static com.hazelcast.jet.core.JobStatus.RESTARTING;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.impl.SnapshotRepository.snapshotDataMapName;
import static com.hazelcast.jet.impl.execution.SnapshotContext.NO_SNAPSHOT;
import static com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject.deserializeWithCustomClassLoader;
import static com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder.createExecutionPlans;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isRestartableException;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.idToString;
import static com.hazelcast.jet.impl.util.Util.jobAndExecutionId;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

/**
 * Data pertaining to single job on master member. There's one instance per job,
 * shared between multiple executions.
 */
public class MasterContext {

    public static final int SNAPSHOT_RESTORE_EDGE_PRIORITY = Integer.MIN_VALUE;

    private final NodeEngineImpl nodeEngine;
    private final JobCoordinationService coordinationService;
    private final ILogger logger;
    private final JobRecord jobRecord;
    private final long jobId;
    private final NonCompletableFuture completionFuture = new NonCompletableFuture();
    private final CompletionToken cancellationToken;
    private final AtomicReference<JobStatus> jobStatus = new AtomicReference<>(NOT_STARTED);
    private final SnapshotRepository snapshotRepository;
    private volatile Set<Vertex> vertices;

    private volatile long executionId;
    private volatile long jobStartTime;
    private volatile Map<MemberInfo, ExecutionPlan> executionPlanMap;
    private volatile CompletionToken executionRestartToken;

    MasterContext(NodeEngineImpl nodeEngine, JobCoordinationService coordinationService, JobRecord jobRecord) {
        this.nodeEngine = nodeEngine;
        this.coordinationService = coordinationService;
        this.snapshotRepository = coordinationService.snapshotRepository();
        this.logger = nodeEngine.getLogger(getClass());
        this.jobRecord = jobRecord;
        this.jobId = jobRecord.getJobId();
        this.cancellationToken = new CompletionToken(logger);
    }

    public long getJobId() {
        return jobId;
    }

    public long getExecutionId() {
        return executionId;
    }

    public JobStatus jobStatus() {
        return jobStatus.get();
    }

    public JobConfig getJobConfig() {
        return jobRecord.getConfig();
    }

    public JobRecord getJobRecord() {
        return jobRecord;
    }

    public CompletableFuture<Void> completionFuture() {
        return completionFuture;
    }

    boolean cancelJob() {
        return cancellationToken.complete();
    }

    boolean isCancelled() {
        return cancellationToken.isCompleted();
    }

    /**
     * Starts execution of the job if it is not already completed, cancelled or failed.
     * If the job is already cancelled, the job completion procedure is triggered.
     * If the job quorum is not satisfied, job restart is rescheduled.
     * If there was a membership change and the partition table is not completely
     * fixed yet, job restart is rescheduled.
     */
    void tryStartJob(Function<Long, Long> executionIdSupplier) {
        if (!setJobStatusToStarting()) {
            return;
        }

        if (scheduleRestartIfQuorumAbsent() || scheduleRestartIfClusterIsNotSafe()) {
            return;
        }

        DAG dag;
        try {
            dag = deserializeDAG();
        } catch (Exception e) {
            logger.warning("DAG deserialization failed", e);
            finalizeJob(e);
            return;
        }
        // save a copy of the vertex list, because it is going to change
        vertices = new HashSet<>();
        dag.iterator().forEachRemaining(vertices::add);
        executionId = executionIdSupplier.apply(jobId);

        // last started snapshot complete or not complete. The next started snapshot must be greater than this number
        long lastSnapshotId = NO_SNAPSHOT;
        if (isSnapshottingEnabled()) {
            Long snapshotIdToRestore = snapshotRepository.latestCompleteSnapshot(jobId);
            snapshotRepository.deleteAllSnapshotsExceptOne(jobId, snapshotIdToRestore);
            Long lastStartedSnapshot = snapshotRepository.latestStartedSnapshot(jobId);
            if (snapshotIdToRestore != null) {
                logger.info("State of " + jobIdString() + " will be restored from snapshot "
                        + snapshotIdToRestore);
                rewriteDagWithSnapshotRestore(dag, snapshotIdToRestore);
            } else {
                logger.info("No previous snapshot for " + jobIdString() + " found.");
            }
            if (lastStartedSnapshot != null) {
                lastSnapshotId = lastStartedSnapshot;
            }
        }

        MembersView membersView = getMembersView();
        ClassLoader previousCL = swapContextClassLoader(coordinationService.getClassLoader(jobId));
        try {
            logger.info("Start executing " + jobIdString() + ", status " + jobStatus()
                    + ", execution graph in DOT format:\n" + dag.toDotString()
                    + "\nHINT: You can use graphviz or http://viz-js.com to visualize the printed graph.");
            logger.fine("Building execution plan for " + jobIdString());
            executionPlanMap = createExecutionPlans(nodeEngine, membersView, dag, getJobConfig(), lastSnapshotId);
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
        invoke(operationCtor, this::onInitStepCompleted, null);
    }

    private void rewriteDagWithSnapshotRestore(DAG dag, long snapshotId) {
        logger.info(jobIdString() + ": restoring state from snapshotId=" + snapshotId);
        for (Vertex vertex : dag) {
            // We add the vertex even in case when the map is empty: this ensures, that
            // Processor.finishSnapshotRestore() method is always called on all vertices in
            // a job which is restored from a snapshot.
            String mapName = snapshotDataMapName(jobId, snapshotId, vertex.getName());
            Vertex readSnapshotVertex = dag.newVertex("__snapshot_read." + vertex.getName(), readMapP(mapName));
            Vertex explodeVertex = dag.newVertex("__snapshot_explode." + vertex.getName(), ExplodeSnapshotP::new);

            readSnapshotVertex.localParallelism(vertex.getLocalParallelism());
            explodeVertex.localParallelism(vertex.getLocalParallelism());

            int destOrdinal = dag.getInboundEdges(vertex.getName()).size();
            dag.edge(between(readSnapshotVertex, explodeVertex).isolated())
               .edge(new SnapshotRestoreEdge(explodeVertex, vertex, destOrdinal));
        }
    }

    /**
     * Sets job status to starting.
     * Returns false if the job start process cannot proceed.
     */
    private boolean setJobStatusToStarting() {
        JobStatus status = jobStatus();
        if (status == COMPLETED || status == FAILED) {
            logger.severe("Cannot init job " + idToString(jobId) + ": it is already " + status);
            return false;
        }

        if (cancellationToken.isCompleted()) {
            logger.fine("Skipping init job " + idToString(jobId) + ": is already cancelled.");
            finalizeJob(new CancellationException());
            return false;
        }

        if (status == NOT_STARTED) {
            if (!jobStatus.compareAndSet(NOT_STARTED, STARTING)) {
                logger.fine("Cannot init job " + idToString(jobId) + ": someone else is just starting it");
                return false;
            }

            jobStartTime = System.currentTimeMillis();
        }

        status = jobStatus();
        if (!(status == STARTING || status == RESTARTING)) {
            logger.severe("Cannot init job " + idToString(jobId) + ": status is " + status);
            return false;
        }

        return true;
    }

    private boolean scheduleRestartIfQuorumAbsent() {
        int quorumSize = jobRecord.getQuorumSize();
        if (coordinationService.isQuorumPresent(quorumSize)) {
            return false;
        }

        logger.fine("Rescheduling restart of job " + idToString(jobId) + ": quorum size " + quorumSize + " is not met");
        scheduleRestart();
        return true;
    }

    private boolean scheduleRestartIfClusterIsNotSafe() {
        if (coordinationService.shouldStartJobs()) {
            return false;
        }

        logger.fine("Rescheduling restart of job " + idToString(jobId) + ": cluster is not safe");
        scheduleRestart();
        return true;
    }

    private void scheduleRestart() {
        jobStatus.compareAndSet(RUNNING, RESTARTING);
        coordinationService.scheduleRestart(jobId);
    }

    private MembersView getMembersView() {
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
        return clusterService.getMembershipManager().getMembersView();
    }

    private DAG deserializeDAG() {
        ClassLoader cl = coordinationService.getClassLoader(jobId);
        return deserializeWithCustomClassLoader(nodeEngine.getSerializationService(), cl, jobRecord.getDag());
    }

    // Called as callback when all InitOperation invocations are done
    private void onInitStepCompleted(Map<MemberInfo, Object> responses) {
        Throwable error = getInitResult(responses);

        if (error == null) {
            JobStatus status = jobStatus();

            if (!(status == STARTING || status == RESTARTING)) {
                error = new IllegalStateException("Cannot execute " + jobIdString()
                        + ": status is " + status);
            }
        }

        if (error == null) {
            invokeStartExecution();
        } else {
            invokeCompleteExecution(error);
        }
    }

    /**
     * If there is no failure, then returns null. If the job is cancelled, then returns CancellationException.
     * If there is at least one non-restartable failure, such as an exception in user code, then returns that failure.
     * Otherwise, the failure is because a job participant has left the cluster.
     * In that case, TopologyChangeException is returned so that the job will be restarted.
     */
    private Throwable getInitResult(Map<MemberInfo, Object> responses) {
        if (cancellationToken.isCompleted()) {
            logger.fine(jobIdString() + " to be cancelled after init");
            return new CancellationException();
        }

        Map<Boolean, List<Entry<MemberInfo, Object>>> grouped = groupResponses(responses);
        Collection<MemberInfo> successfulMembers = grouped.get(false).stream().map(Entry::getKey).collect(toList());

        if (successfulMembers.size() == executionPlanMap.size()) {
            logger.fine("Init of " + jobIdString() + " is successful.");
            return null;
        }

        List<Entry<MemberInfo, Object>> failures = grouped.get(true);
        logger.fine("Init of " + jobIdString() + " failed with: " + failures);

        // if there is at least one non-restartable failure, such as a user code failure, then fail the job
        // otherwise, return TopologyChangedException so that the job will be restarted
        return failures
                .stream()
                .map(e -> (Throwable) e.getValue())
                .filter(t -> !isRestartableException(t))
                .findFirst()
                .map(ExceptionUtil::peel)
                .orElse(new TopologyChangedException());
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

        ExecutionInvocationCallback callback = new ExecutionInvocationCallback(executionId);

        cancellationToken.whenCompleted(callback::cancelInvocations);

        CompletionToken executionRestartToken = new CompletionToken(logger);
        executionRestartToken.whenCompleted(callback::cancelInvocations);

        Function<ExecutionPlan, Operation> operationCtor = plan -> new StartExecutionOperation(jobId, executionId);
        Consumer<Map<MemberInfo, Object>> completionCallback = results -> {
            this.executionRestartToken = null;
            onExecuteStepCompleted(results, executionRestartToken.isCompleted());
        };

        // We must set executionRestartToken before we call invoke() method because once all invocations
        // are done, executionRestartToken will be reset. Therefore, setting it after the invoke() call is racy.
        this.executionRestartToken = executionRestartToken;
        jobStatus.set(RUNNING);

        invoke(operationCtor, completionCallback, callback);

        if (isSnapshottingEnabled()) {
            coordinationService.scheduleSnapshot(jobId, executionId);
        }
    }

    private void cancelExecutionInvocations(long jobId, long executionId) {
        nodeEngine.getExecutionService().execute(ExecutionService.ASYNC_EXECUTOR, () -> {
            Function<ExecutionPlan, Operation> operationCtor = plan -> new CancelExecutionOperation(jobId, executionId);
            invoke(operationCtor, responses -> { }, null);
        });
    }

    /**
     * Cancels the job execution invocations in order to restart it afterwards if the job is currently being executed
     */
    boolean restartExecution() {
        CompletionToken restartToken = this.executionRestartToken;
        if (restartToken != null) {
            restartToken.complete();
            return true;
        }

        return false;
    }

    void beginSnapshot(long executionId) {
        if (this.executionId != executionId) {
            // current execution is completed and probably a new execution has started
            logger.warning("Not beginning snapshot since expected execution id " + idToString(this.executionId)
                    + " does not match to " + jobAndExecutionId(jobId, executionId));
            return;
        }

        List<String> vertexNames = vertices.stream().map(Vertex::getName).collect(Collectors.toList());
        long newSnapshotId = snapshotRepository.registerSnapshot(jobId, vertexNames);

        logger.info(String.format("Starting snapshot %s for %s", newSnapshotId, jobAndExecutionId(jobId, executionId)));
        Function<ExecutionPlan, Operation> factory =
                plan -> new SnapshotOperation(jobId, executionId, newSnapshotId);

        invoke(factory, responses -> onSnapshotCompleted(responses, executionId, newSnapshotId), null);
    }

    private void onSnapshotCompleted(Map<MemberInfo, Object> responses, long executionId, long snapshotId) {
        SnapshotOperationResult mergedResult = new SnapshotOperationResult();
        for (Object response : responses.values()) {
            mergedResult.merge((SnapshotOperationResult) response);
        }

        boolean isSuccess = mergedResult.getError() == null;
        if (!isSuccess) {
            logger.warning(jobAndExecutionId(jobId, executionId) + " snapshot " + snapshotId + " has failure, " +
                    "first failure: " + mergedResult.getError());
        }
        coordinationService.completeSnapshot(jobId, executionId, snapshotId, isSuccess,
                mergedResult.getNumBytes(), mergedResult.getNumKeys(), mergedResult.getNumChunks());
    }

    // Called as callback when all ExecuteOperation invocations are done
    private void onExecuteStepCompleted(Map<MemberInfo, Object> responses, boolean isRestartRequested) {
        invokeCompleteExecution(getExecuteResult(responses, isRestartRequested));
    }

    /**
     * <ul>
     * <li>Returns null if there is no failure.
     * <li>Returns CancellationException if the job is cancelled.
     * <li>Returns JobRestartRequestedException if the current execution is cancelled
     * <li>If there is at least one non-restartable failure, such as an exception in user code, then returns that failure.
     * <li>Otherwise, the failure is because a job participant has left the cluster.
     *   In that case, {@code TopologyChangeException} is returned so that the job will be restarted.
     * </ul>
     */
    private Throwable getExecuteResult(Map<MemberInfo, Object> responses, boolean isRestartRequested) {
        if (cancellationToken.isCompleted()) {
            logger.fine(jobIdString() + " to be cancelled after execute");
            return new CancellationException();
        } else if (isRestartRequested) {
            return new JobRestartRequestedException();
        }

        Map<Boolean, List<Entry<MemberInfo, Object>>> grouped = groupResponses(responses);
        Collection<MemberInfo> successfulMembers = grouped.get(false).stream().map(Entry::getKey).collect(toList());

        if (successfulMembers.size() == executionPlanMap.size()) {
            logger.fine("Execute of " + jobIdString() + " is successful.");
            return null;
        }

        List<Entry<MemberInfo, Object>> failures = grouped.get(true);
        logger.fine("Execute of " + jobIdString() + " has failures: " + failures);

        // If there is no user-code exception, it means at least one job participant has left the cluster.
        // In that case, all remaining participants return a CancellationException.
        return failures
                .stream()
                .map(e -> (Throwable) e.getValue())
                .filter(t -> !(t instanceof CancellationException || isRestartableException(t)))
                .findFirst()
                .map(ExceptionUtil::peel)
                .orElse(new TopologyChangedException());
    }

    private void invokeCompleteExecution(Throwable error) {
        JobStatus status = jobStatus();

        Throwable finalError;
        if (status == STARTING || status == RESTARTING || status == RUNNING) {
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
        invoke(operationCtor, responses -> finalizeJob(error), null);
    }

    // Called as callback when all CompleteOperation invocations are done
    private void finalizeJob(@Nullable Throwable failure) {
        if (assertJobNotAlreadyDone(failure)) {
            return;
        }

        completeVertices(failure);

        if (shouldRestart(failure)) {
            scheduleRestart();
            return;
        }

        long elapsed = System.currentTimeMillis() - jobStartTime;
        if (isSuccess(failure)) {
            logger.info(String.format("Execution of %s completed in %,d ms", jobIdString(), elapsed));
        } else {
            logger.warning(String.format("Execution of %s failed after %,d ms", jobIdString(), elapsed), failure);
        }

        JobStatus status = isSuccess(failure) ? COMPLETED : FAILED;
        jobStatus.set(status);

        try {
            coordinationService.completeJob(this, executionId, System.currentTimeMillis(), failure);
        } catch (RuntimeException e) {
            logger.warning("Completion of " + jobIdString() + " failed", e);
        } finally {
            setFinalResult(failure);
        }
    }

    private boolean assertJobNotAlreadyDone(@Nullable Throwable failure) {
        JobStatus status = jobStatus();
        if (status == COMPLETED || status == FAILED) {
            if (failure != null) {
                logger.severe("Ignoring failure completion of " + idToString(jobId) + " because status is "
                        + status, failure);
            } else {
                logger.severe("Ignoring completion of " + idToString(jobId) + " because status is " + status);
            }
            return true;
        }
        return false;
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

    private boolean shouldRestart(Throwable t) {
        return t instanceof JobRestartRequestedException
                || (t instanceof RestartableException || t instanceof TopologyChangedException)
                        && jobRecord.getConfig().isAutoRestartOnMemberFailureEnabled();
    }

    void setFinalResult(Throwable failure) {
        if (failure == null) {
            completionFuture.internalComplete();
        } else {
            completionFuture.internalCompleteExceptionally(failure);
        }
    }

    private boolean isSuccess(Throwable failure) {
        return (failure == null || failure instanceof CancellationException);
    }

    private void invoke(Function<ExecutionPlan, Operation> operationCtor,
                        Consumer<Map<MemberInfo, Object>> completionCallback,
                        ExecutionCallback<Object> callback) {
        CompletableFuture<Void> doneFuture = new CompletableFuture<>();
        Map<MemberInfo, InternalCompletableFuture<Object>> futures = new ConcurrentHashMap<>();
        invokeOnParticipants(futures, doneFuture, operationCtor);

        // once all invocations return, notify the completion callback
        doneFuture.whenComplete(withTryCatch(logger, (aVoid, throwable) -> {
            Map<MemberInfo, Object> responses = new HashMap<>();
            for (Entry<MemberInfo, InternalCompletableFuture<Object>> entry : futures.entrySet()) {
                Object val;
                try {
                    val = entry.getValue().get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    val = e;
                } catch (Exception e) {
                    val = peel(e);
                }
                responses.put(entry.getKey(), val);
            }
            completionCallback.accept(responses);
        }));

        if (callback != null) {
            futures.values().forEach(f -> f.andThen(callback));
        }
    }

    private void invokeOnParticipants(Map<MemberInfo, InternalCompletableFuture<Object>> futures,
                                      CompletableFuture<Void> doneFuture,
                                      Function<ExecutionPlan, Operation> opCtor) {
        AtomicInteger remainingCount = new AtomicInteger(executionPlanMap.size());
        for (Entry<MemberInfo, ExecutionPlan> e : executionPlanMap.entrySet()) {
            MemberInfo member = e.getKey();
            Operation op = opCtor.apply(e.getValue());
            InternalCompletableFuture<Object> future = nodeEngine.getOperationService()
                 .createInvocationBuilder(JetService.SERVICE_NAME, op, member.getAddress())
                 .setDoneCallback(() -> {
                     if (remainingCount.decrementAndGet() == 0) {
                         doneFuture.complete(null);
                     }
                 })
                 .invoke();
            futures.put(member, future);
        }
    }

    private boolean isSnapshottingEnabled() {
        return getJobConfig().getProcessingGuarantee() != ProcessingGuarantee.NONE;
    }

    private String jobIdString() {
        return jobAndExecutionId(jobId, executionId);
    }

    private static ClassLoader swapContextClassLoader(ClassLoader jobClassLoader) {
        Thread currentThread = Thread.currentThread();
        ClassLoader contextClassLoader = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(jobClassLoader);
        return contextClassLoader;
    }

    /**
     * Specific type of edge to be used when restoring snapshots
     */
    private static class SnapshotRestoreEdge extends Edge {

        SnapshotRestoreEdge(Vertex source, Vertex destination, int destOrdinal) {
            super(source, 0, destination, destOrdinal);
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
            cancelInvocations();
        }

        void cancelInvocations() {
            if (invocationsCancelled.compareAndSet(false, true)) {
                cancelExecutionInvocations(jobId, executionId);
            }
        }
    }

}
