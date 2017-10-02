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

package com.hazelcast.jet.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.impl.execution.BroadcastEntry;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder;
import com.hazelcast.jet.impl.operation.CompleteOperation;
import com.hazelcast.jet.impl.operation.ExecuteOperation;
import com.hazelcast.jet.impl.operation.InitOperation;
import com.hazelcast.jet.impl.operation.SnapshotOperation;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.NOT_STARTED;
import static com.hazelcast.jet.core.JobStatus.RESTARTING;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMap;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.impl.SnapshotRepository.snapshotDataMapName;
import static com.hazelcast.jet.impl.execution.SnapshotContext.NO_SNAPSHOT;
import static com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject.deserializeWithCustomClassLoader;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isTopologicalFailure;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.idToString;
import static com.hazelcast.jet.impl.util.Util.jobAndExecutionId;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

public class MasterContext {

    public static final int SNAPSHOT_RESTORE_EDGE_PRIORITY = Integer.MIN_VALUE;

    private final NodeEngineImpl nodeEngine;
    private final JobCoordinationService coordinationService;
    private final ILogger logger;
    private final JobRecord jobRecord;
    private final long jobId;
    private final CompletableFuture<Boolean> completionFuture = new CompletableFuture<>();
    private final AtomicReference<JobStatus> jobStatus = new AtomicReference<>(NOT_STARTED);
    private final SnapshotRepository snapshotRepository;
    private volatile Set<String> vertexNames;

    private volatile long executionId;
    private volatile long jobStartTime;
    private volatile Map<MemberInfo, ExecutionPlan> executionPlanMap;

    MasterContext(NodeEngineImpl nodeEngine, JobCoordinationService coordinationService, JobRecord jobRecord) {
        this.nodeEngine = nodeEngine;
        this.coordinationService = coordinationService;
        this.snapshotRepository = coordinationService.snapshotRepository();
        this.logger = nodeEngine.getLogger(getClass());
        this.jobRecord = jobRecord;
        this.jobId = jobRecord.getJobId();
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

    CompletableFuture<Boolean> completionFuture() {
        return completionFuture;
    }

    boolean cancel() {
        return completionFuture.cancel(true);
    }

    boolean isCancelled() {
        return completionFuture.isCancelled();
    }

    /**
     * Starts execution of the job if it is not already completed, cancelled or failed.
     * If the job is already cancelled, the job completion procedure is triggered.
     * If the job quorum is not satisfied, job restart is rescheduled.
     * If there was a membership change and the partition table is not completely fixed yet, job restart is rescheduled.
     */
    void tryStartJob(Function<Long, Long> executionIdSupplier) {
        if (!setJobStatusToStarting()) {
            return;
        }

        if (scheduleRestartIfQuorumAbsent() || scheduleRestartIfClusterIsNotSafe()) {
            return;
        }

        DAG dag = deserializeDAG();
        // save a copy of the vertex list, because it is going to change
        vertexNames = new HashSet<>();
        dag.iterator().forEachRemaining(e1 -> vertexNames.add(e1.getName()));
        executionId = executionIdSupplier.apply(jobId);

        // last started snapshot complete or not complete. The next started snapshot must be greater than this number
        long lastSnapshotId = NO_SNAPSHOT;
        if (jobRecord.getConfig().getSnapshotInterval() > 0) {
            Long snapshotIdToRestore = snapshotRepository.latestCompleteSnapshot(jobId);
            snapshotRepository.deleteAllSnapshotsExceptOne(jobId, snapshotIdToRestore);
            Long lastStartedSnapshot = snapshotRepository.latestStartedSnapshot(jobId);
            if (snapshotIdToRestore != null) {
                logger.info("State of " + jobAndExecutionId(jobId, executionId) + " will be restored from snapshot "
                        + snapshotIdToRestore);
                rewriteDagWithSnapshotRestore(dag, snapshotIdToRestore);
            } else {
                logger.warning("No usable snapshot for " + jobAndExecutionId(jobId, executionId) + " found.");
            }
            if (lastStartedSnapshot != null) {
                lastSnapshotId = lastStartedSnapshot;
            }
        }

        MembersView membersView = getMembersView();
        try {
            logger.info("Start executing " + jobAndExecutionId(jobId, executionId) + ", status " + jobStatus()
                    + "\n" + dag);
            logger.fine("Building execution plan for " + jobAndExecutionId(jobId, executionId));
            JobConfig jobConfig = jobRecord.getConfig();
            executionPlanMap = ExecutionPlanBuilder.createExecutionPlans(nodeEngine,
                    membersView, dag, jobConfig, lastSnapshotId);
        } catch (TopologyChangedException e) {
            logger.severe("Execution plans could not be created for " + jobAndExecutionId(jobId, executionId), e);
            scheduleRestart();
            return;
        }

        logger.fine("Built execution plans for " + jobAndExecutionId(jobId, executionId));
        Set<MemberInfo> participants = executionPlanMap.keySet();
        Function<ExecutionPlan, Operation> operationCtor = plan ->
                new InitOperation(jobId, executionId, membersView.getVersion(), participants, plan);
        invoke(operationCtor, this::onInitStepCompleted, null);
    }

    private void rewriteDagWithSnapshotRestore(DAG dag, long snapshotId) {
        logger.info(jobAndExecutionId(jobId, executionId) + ": restoring state from snapshotId=" + snapshotId);
        for (Vertex vertex : dag) {
            String mapName = snapshotDataMapName(jobId, snapshotId, vertex.getName());
            if (!nodeEngine.getHazelcastInstance().getMap(mapName).isEmpty()) {
                // items with keys of type BroadcastKey need to be broadcast to all processors
                DistributedPredicate<Entry<Object, Object>> predicate = (Entry<Object, Object> e) -> true;
                DistributedFunction<Entry<Object, Object>, ?> projection = (Entry<Object, Object> e) ->
                        (e.getKey() instanceof BroadcastKey) ? new BroadcastEntry<>(e) : e;
                Vertex readSnapshotVertex = dag.newVertex("__read_snapshot." + vertex.getName(),
                        readMap(mapName, predicate, projection));

                readSnapshotVertex.localParallelism(vertex.getLocalParallelism());

                int destOrdinal = dag.getInboundEdges(vertex.getName()).size();
                dag.edge(new SnapshotRestoreEdge(readSnapshotVertex, vertex, destOrdinal));
            }
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

        if (completionFuture.isCancelled()) {
            logger.fine("Skipping init job " + idToString(jobId) + ": is already cancelled.");
            onCompleteStepCompleted(null);
            return false;
        }

        if (status == NOT_STARTED) {
            if (!jobStatus.compareAndSet(NOT_STARTED, STARTING)) {
                logger.fine("Cannot init job " + idToString(jobId) + ": someone else is just starting it");
                return false;
            }

            jobStartTime = System.currentTimeMillis();
        } else {
            jobStatus.compareAndSet(RUNNING, RESTARTING);
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
                error = new IllegalStateException("Cannot execute " + jobAndExecutionId(jobId, executionId)
                        + ": status is " + status);
            }
        }

        if (error == null) {
            invokeExecute();
        } else {
            invokeComplete(error);
        }
    }

    /**
     * If there is no failure, then returns null. If the job is cancelled, then returns CancellationException.
     * If there is at least one non-restartable failure, such as an exception in user code, then returns that failure.
     * Otherwise, the failure is because a job participant has left the cluster.
     * In that case, TopologyChangeException is returned so that the job will be restarted.
     */
    private Throwable getInitResult(Map<MemberInfo, Object> responses) {
        if (completionFuture.isCancelled()) {
            logger.fine(jobAndExecutionId(jobId, executionId) + " to be cancelled after init");
            return new CancellationException();
        }

        Map<Boolean, List<Entry<MemberInfo, Object>>> grouped = groupResponses(responses);
        Collection<MemberInfo> successfulMembers = grouped.get(false).stream().map(Entry::getKey).collect(toList());

        if (successfulMembers.size() == executionPlanMap.size()) {
            logger.fine("Init of " + jobAndExecutionId(jobId, executionId) + " is successful.");
            return null;
        }

        List<Entry<MemberInfo, Object>> failures = grouped.get(true);
        logger.fine("Init of " + jobAndExecutionId(jobId, executionId) + " failed with: " + failures);

        // if there is at least one non-restartable failure, such as a user code failure, then fail the job
        // otherwise, return TopologyChangedException so that the job will be restarted
        return failures
                .stream()
                .map(e -> (Throwable) e.getValue())
                .filter(t -> !isTopologicalFailure(t))
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
    private void invokeExecute() {
        jobStatus.set(RUNNING);
        logger.fine("Executing " + jobAndExecutionId(jobId, executionId));
        Function<ExecutionPlan, Operation> operationCtor = plan -> new ExecuteOperation(jobId, executionId);
        invoke(operationCtor, this::onExecuteStepCompleted, completionFuture);

        if (getJobConfig().getSnapshotInterval() > 0) {
            coordinationService.scheduleSnapshot(jobId, executionId);
        }
    }

    void beginSnapshot(long executionId) {
        if (this.executionId != executionId) {
            // current execution is completed and probably a new execution has started
            logger.warning("Not beginning snapshot since expected execution id " + idToString(this.executionId)
                    + " does not match to " + jobAndExecutionId(jobId, executionId));
            return;
        }

        long newSnapshotId = snapshotRepository.registerSnapshot(jobId, vertexNames);

        logger.info(String.format("Starting snapshot %s for %s", newSnapshotId, jobAndExecutionId(jobId, executionId)));
        Function<ExecutionPlan, Operation> factory =
                plan -> new SnapshotOperation(jobId, executionId, newSnapshotId);

        invoke(factory, responses -> onSnapshotCompleted(responses, executionId, newSnapshotId), null);
    }

    private void onSnapshotCompleted(Map<MemberInfo, Object> responses, long executionId, long snapshotId) {
        Map<Address, Throwable> errors = responses.entrySet().stream()
            .filter(e -> e.getValue() instanceof Throwable)
            .filter(e -> !(e.getValue() instanceof CancellationException) || !isTopologicalFailure(e.getValue()))
            .collect(Collectors.toMap(e -> e.getKey().getAddress(), e -> (Throwable) e.getValue()));

        boolean isSuccess = errors.isEmpty();
        if (!isSuccess) {
            logger.warning(jobAndExecutionId(jobId, executionId) + " snapshot " + snapshotId + " has failures: "
                    + errors);
        }
        coordinationService.completeSnapshot(jobId, executionId, snapshotId, isSuccess);
    }

    // Called as callback when all ExecuteOperation invocations are done
    private void onExecuteStepCompleted(Map<MemberInfo, Object> responses) {
        invokeComplete(getExecuteResult(responses));
    }

    /**
     * If there is no failure, then returns null. If the job is cancelled, then returns CancellationException.
     * If there is at least one non-restartable failure, such as an exception in user code, then returns that failure.
     * Otherwise, the failure is because a job participant has left the cluster.
     * In that case, TopologyChangeException is returned so that the job will be restarted.
     */
    private Throwable getExecuteResult(Map<MemberInfo, Object> responses) {
        if (completionFuture.isCancelled()) {
            logger.fine(jobAndExecutionId(jobId, executionId) + " to be cancelled after execute");
            return new CancellationException();
        }

        Map<Boolean, List<Entry<MemberInfo, Object>>> grouped = groupResponses(responses);
        Collection<MemberInfo> successfulMembers = grouped.get(false).stream().map(Entry::getKey).collect(toList());

        if (successfulMembers.size() == executionPlanMap.size()) {
            logger.fine("Execute of " + jobAndExecutionId(jobId, executionId) + " is successful.");
            return null;
        }

        List<Entry<MemberInfo, Object>> failures = grouped.get(true);
        logger.fine("Execute of " + jobAndExecutionId(jobId, executionId) + " has failures: " + failures);

        // If there is no user-code exception, it means at least one job participant has left the cluster.
        // In that case, all remaining participants return a CancellationException.
        return failures
                .stream()
                .map(e -> (Throwable) e.getValue())
                .filter(t -> !(t instanceof CancellationException || isTopologicalFailure(t)))
                .findFirst()
                .map(ExceptionUtil::peel)
                .orElse(new TopologyChangedException());
    }

    private void invokeComplete(Throwable error) {
        JobStatus status = jobStatus();

        Throwable finalError;
        if (status == STARTING || status == RESTARTING || status == RUNNING) {
            logger.fine("Completing " + jobAndExecutionId(jobId, executionId));
            finalError = error;
        } else {
            if (error != null) {
                logger.severe("Cannot properly complete failed " + jobAndExecutionId(jobId, executionId)
                        + ": status is " + status, error);
            } else {
                logger.severe("Cannot properly complete " + jobAndExecutionId(jobId, executionId)
                        + ": status is " + status);
            }

            finalError = new IllegalStateException("Job coordination failed.");
        }

        Function<ExecutionPlan, Operation> operationCtor = plan -> new CompleteOperation(executionId, finalError);
        invoke(operationCtor, responses -> onCompleteStepCompleted(error), null);
    }

    // Called as callback when all CompleteOperation invocations are done
    private void onCompleteStepCompleted(@Nullable Throwable failure) {
        JobStatus status = jobStatus();
        if (status == COMPLETED || status == FAILED) {
            if (failure != null) {
                logger.severe("Ignoring failure completion of " + idToString(jobId) + " because status is "
                        + status, failure);
            } else {
                logger.severe("Ignoring completion of " + idToString(jobId) + " because status is " + status);
            }
            return;
        }

        long completionTime = System.currentTimeMillis();
        if (failure instanceof TopologyChangedException && jobRecord.getConfig().isAutoRestartOnMemberFailureEnabled()) {
            scheduleRestart();
            return;
        }

        long elapsed = completionTime - jobStartTime;
        if (isSuccess(failure)) {
            logger.info("Execution of " + jobAndExecutionId(jobId, executionId) + " completed in " + elapsed + " ms");
        } else {
            logger.warning("Execution of " + jobAndExecutionId(jobId, executionId)
                    + " failed in " + elapsed + " ms", failure);
        }

        try {
            coordinationService.completeJob(this, executionId, completionTime, failure);
        } catch (RuntimeException e) {
            logger.warning("Completion of " + jobAndExecutionId(jobId, executionId)
                    + " failed in " + elapsed + " ms", failure);
        } finally {
            setFinalResult(failure);
        }
    }

    void setFinalResult(Throwable failure) {
        JobStatus status = isSuccess(failure) ? COMPLETED : FAILED;
        jobStatus.set(status);
        if (status == COMPLETED) {
            completionFuture.complete(true);
        } else {
            completionFuture.completeExceptionally(failure);
        }
    }

    private boolean isSuccess(Throwable failure) {
        return (failure == null || failure instanceof CancellationException);
    }

    private void invoke(Function<ExecutionPlan, Operation> operationCtor,
                        Consumer<Map<MemberInfo, Object>> completionCallback,
                        CompletableFuture cancellationFuture) {
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

        boolean cancelOnFailure = (cancellationFuture != null);

        // if cancelOnFailure is true, we should cancel invocations when the future is cancelled, or any invocation fail

        if (cancelOnFailure) {
            cancellationFuture.whenComplete(withTryCatch(logger, (r, e) -> {
                if (e instanceof CancellationException) {
                    futures.values().forEach(f -> f.cancel(true));
                }
            }));

            ExecutionCallback<Object> callback = new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                }

                @Override
                public void onFailure(Throwable t) {
                    futures.values().forEach(f -> f.cancel(true));
                }
            };

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
}
