/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.core.LocalMemberResetException;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.metrics.impl.MetricDescriptorImpl;
import com.hazelcast.internal.metrics.impl.MetricsCompressor;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.function.RunnableEx;
import com.hazelcast.jet.impl.TerminationMode.ActionAfterTerminate;
import com.hazelcast.jet.impl.exception.CancellationByUserException;
import com.hazelcast.jet.impl.exception.ExecutionNotFoundException;
import com.hazelcast.jet.impl.exception.JetDisabledException;
import com.hazelcast.jet.impl.exception.JobTerminateRequestedException;
import com.hazelcast.jet.impl.exception.TerminatedWithSnapshotException;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder;
import com.hazelcast.jet.impl.metrics.RawJobMetrics;
import com.hazelcast.jet.impl.operation.GetLocalExecutionMetricsOperation;
import com.hazelcast.jet.impl.operation.InitExecutionOperation;
import com.hazelcast.jet.impl.operation.StartExecutionOperation;
import com.hazelcast.jet.impl.operation.TerminateExecutionOperation;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.jet.impl.util.NonCompletableFuture;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.version.Version;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.hazelcast.function.Functions.entryKey;
import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.Util.entry;
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
import static com.hazelcast.jet.impl.JobClassLoaderService.JobPhase.COORDINATOR;
import static com.hazelcast.jet.impl.JobRepository.exportedSnapshotMapName;
import static com.hazelcast.jet.impl.SnapshotValidator.validateSnapshot;
import static com.hazelcast.jet.impl.TerminationMode.ActionAfterTerminate.RESTART;
import static com.hazelcast.jet.impl.TerminationMode.ActionAfterTerminate.SUSPEND;
import static com.hazelcast.jet.impl.TerminationMode.CANCEL_FORCEFUL;
import static com.hazelcast.jet.impl.TerminationMode.CANCEL_GRACEFUL;
import static com.hazelcast.jet.impl.TerminationMode.RESTART_FORCEFUL;
import static com.hazelcast.jet.impl.TerminationMode.RESTART_GRACEFUL;
import static com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject.deserializeWithCustomClassLoader;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isRestartableException;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isTopologyException;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static com.hazelcast.jet.impl.util.Util.doWithClassLoader;
import static com.hazelcast.jet.impl.util.Util.formatJobDuration;
import static com.hazelcast.jet.impl.util.Util.isJobSuspendable;
import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.JOB_OFFLOADABLE_EXECUTOR;
import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * Part of {@link MasterContext} that deals with execution starting and
 * termination.
 */
public class MasterJobContext {

    public static final int SNAPSHOT_RESTORE_EDGE_PRIORITY = Integer.MIN_VALUE;
    public static final String SNAPSHOT_VERTEX_PREFIX = "__snapshot_";

    private static final int COLLECT_METRICS_RETRY_DELAY_MILLIS = 100;
    private static final Runnable NO_OP = () -> { };

    private final MasterContext mc;
    private final ILogger logger;
    private final int defaultParallelism;
    private final int defaultQueueSize;

    private volatile long executionStartTime = System.currentTimeMillis();
    private volatile ExecutionFailureCallback executionFailureCallback;
    private volatile Set<Vertex> vertices;
    private volatile boolean verticesCompleted;

    /** Execution-level metrics. Available after the job finishes. */
    @Nonnull
    private volatile List<RawJobMetrics> finalExecutionMetrics = emptyList();

    /** Job-level metrics. Nonnull iff metrics are enabled. */
    private volatile RawJobMetrics jobMetrics;

    private final MetricDescriptor statusMetricDescriptor;
    private final MetricDescriptor userCancelledMetricDescriptor;

    /**
     * A new instance is (re)assigned when the execution is started and
     * completed when its execution ends. Execution ending doesn't mean the job
     * is done, it may be just temporarily stopping due to suspension, job
     * restarting, etc.
     * <p>
     * It's always completed normally, even if the execution fails.
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
     * Current execution termination request in progress, if any. There can be at
     * most one active request at any given time. Cleared when execution is
     * terminated.
     * <p>
     * Members of {@link TerminationRequest} can be accessed directly through
     * this field only when guarded by {@link MasterContext#lock()}, otherwise
     * inconsistent values are possible in subsequent reads. Standard access
     * pattern should involve invocation of {@link #getTerminationRequest()}
     * once and using returned value.
     */
    private volatile TerminationRequest terminationRequest;

    MasterJobContext(MasterContext masterContext, ILogger logger) {
        mc = masterContext;
        this.logger = logger;
        defaultParallelism = mc.getJetServiceBackend().getJetConfig().getCooperativeThreadCount();
        defaultQueueSize = mc.getJetServiceBackend().getJetConfig()
                .getDefaultEdgeConfig().getQueueSize();
        MetricDescriptorImpl jobMetricDescriptor = DEFAULT_DESCRIPTOR_SUPPLIER.get()
                .withTag(MetricTags.JOB, mc.jobIdString())
                .withTag(MetricTags.JOB_NAME, mc.jobName());
        statusMetricDescriptor = jobMetricDescriptor.copy()
                .withMetric(MetricNames.JOB_STATUS)
                .withUnit(ProbeUnit.ENUM);
        userCancelledMetricDescriptor = jobMetricDescriptor.copy()
                .withMetric(MetricNames.IS_USER_CANCELLED)
                .withUnit(ProbeUnit.BOOLEAN);

        // Set initial status metrics
        setJobMetrics(mc.jobStatus(), false);
    }

    public CompletableFuture<Void> jobCompletionFuture() {
        return jobCompletionFuture;
    }

    Optional<TerminationRequest> getTerminationRequest() {
        return Optional.ofNullable(terminationRequest);
    }

    Optional<TerminationMode> requestedTerminationMode() {
        return getTerminationRequest().map(TerminationRequest::getMode);
    }

    boolean isUserInitiatedTermination() {
        return getTerminationRequest().map(TerminationRequest::isUserInitiated).orElse(false);
    }

    /**
     * Creates exception with appropriate type for job cancellation.
     */
    private RuntimeException createCancellationException() {
        return isUserInitiatedTermination() ? new CancellationByUserException() : new CancellationException();
    }

    private boolean isCancelled() {
        return requestedTerminationMode()
                .map(mode -> mode == CANCEL_FORCEFUL)
                .orElse(false);
    }

    private boolean isCancelledGracefully() {
        return requestedTerminationMode()
                .map(mode -> mode == CANCEL_GRACEFUL)
                .orElse(false);
    }

    /**
     * Starts the execution of the job if it is not already completed,
     * cancelled or failed.
     * <p>
     * If the job is already cancelled, triggers the job completion procedure.
     * <p>
     * If the job quorum is not satisfied, reschedules the job restart.
     * <p>
     * If there was a membership change and the partition table is not completely
     * fixed yet, reschedules the job restart.
     */
    void tryStartJob() {
        JobCoordinationService coordinator = mc.coordinationService();
        MembersView membersView = Util.getMembersView(mc.nodeEngine());

        mc.coordinationService()
          .submitToCoordinatorThread(() -> {
              try {
                  executionStartTime = System.currentTimeMillis();
                  JobExecutionRecord jobExecRec = mc.jobExecutionRecord();
                  jobExecRec.markExecuted();
                  DAG dag = resolveDag();
                  if (dag == null) {
                      return;
                  }
                  // must call this before rewriteDagWithSnapshotRestore()
                  String dotRepresentation = dag.toDotString(defaultParallelism, defaultQueueSize);
                  // we ensured that JobExecutionRecord is safe in resolveDag

                  final long snapshotId = jobExecRec.snapshotId();
                  // name without internal prefix
                  final String snapshotName;
                  final String snapshotMapName;

                  // Check if there is a snapshot to restore. We use snapshots in this order:
                  // 1. exported terminal snapshot when the job is restarted due to failure during it
                  // 2. most recent automatic snapshot
                  // 3. configured initialSnapshotName
                  // 4. no snapshot
                  if (snapshotId >= 0) {
                      // job restarts with existing snapshot: could be automatic or exported terminal.
                      // Initial snapshot from Job config, if any, is ignored, because the job has made
                      // its own snapshots.
                      snapshotName = jobExecRec.exportedSnapshotName();
                      snapshotMapName = jobExecRec.successfulSnapshotDataMapName();
                  } else {
                      // there was no snapshot performed before restart or this is a new job
                      snapshotName = mc.jobConfig().getInitialSnapshotName();
                      snapshotMapName = snapshotName != null
                              ? exportedSnapshotMapName(snapshotName)
                              : null;
                  }

                  if (snapshotMapName != null) {
                      rewriteDagWithSnapshotRestore(dag, snapshotId, snapshotMapName, snapshotName);
                  } else {
                      logger.info("Didn't find any snapshot to restore for " + mc.jobIdString());
                  }
                  logger.info("Start executing " + mc.jobIdString()
                          + ", execution graph in DOT format:\n" + dotRepresentation
                          + "\nHINT: You can use graphviz or http://viz-js.com to visualize the printed graph.");
                  logger.fine("Building execution plan for " + mc.jobIdString());

                  createExecutionPlans(dag, membersView)
                          .thenCompose(plans -> coordinator.submitToCoordinatorThread(
                                  () -> initExecution(membersView, plans)
                          ))
                          .whenComplete((r, e) -> {
                              if (e != null) {
                                  finalizeExecution(peel(e));
                              }
                          });
              } catch (Throwable e) {
                  finalizeExecution(e);
              }
          });

        if (mc.hasTimeout()) {
            long remainingTime = mc.remainingTime(Clock.currentTimeMillis());
            mc.coordinationService().scheduleJobTimeout(mc.jobId(), Math.max(1, remainingTime));
        }
    }

    private CompletableFuture<Map<MemberInfo, ExecutionPlan>> createExecutionPlans(
            DAG dag,
            MembersView membersView) {
        return ExecutionPlanBuilder.createExecutionPlans(mc.nodeEngine(), membersView.getMembers(),
                dag, mc.jobId(), mc.executionId(), mc.jobConfig(), mc.jobExecutionRecord().ongoingSnapshotId(),
                false, mc.jobRecord().getSubject());
    }

    private void initExecution(MembersView membersView, Map<MemberInfo, ExecutionPlan> executionPlanMap) {
        mc.setExecutionPlanMap(executionPlanMap);
        logger.fine("Built execution plans for " + mc.jobIdString());
        Set<MemberInfo> participants = mc.executionPlanMap().keySet();
        Version coordinatorVersion = mc.nodeEngine().getLocalMember().getVersion().asVersion();
        Function<ExecutionPlan, Operation> operationCtor = plan ->
                new InitExecutionOperation(mc.jobId(), mc.executionId(), membersView.getVersion(), coordinatorVersion,
                        participants, mc.nodeEngine().getSerializationService().toData(plan), false);
        mc.invokeOnParticipants(operationCtor, this::onInitStepCompleted, null, false);
    }

    @Nullable
    private DAG resolveDag() {
        mc.lock();
        try {
            if (isCancelled()) {
                logger.fine("Skipping init job '" + mc.jobName() + "': is already cancelled.");
                throw createCancellationException();
            }
            if (mc.jobStatus() != NOT_RUNNING) {
                logger.fine("Not starting job '" + mc.jobName() + "': status is " + mc.jobStatus());
                return null;
            }
            if (mc.jobExecutionRecord().isSuspended()) {
                mc.jobExecutionRecord().clearSuspended();
                mc.writeJobExecutionRecord(false);
            }
            if (scheduleRestartIfQuorumAbsent() || scheduleRestartIfClusterIsNotSafe()) {
                return null;
            }
            Version jobClusterVersion = mc.jobRecord().getClusterVersion();
            Version currentClusterVersion = mc.nodeEngine().getClusterService().getClusterVersion();
            if (!jobClusterVersion.equals(currentClusterVersion)) {
                throw new JetException("Cancelling job " + mc.jobName() + ": the cluster was upgraded since the job was "
                        + "submitted. Submitted to version: " + jobClusterVersion + ", current cluster version: "
                        + currentClusterVersion);
            }
            mc.setJobStatus(STARTING);

            // ensure JobExecutionRecord exists
            // we do it in a safe way here, so this does not have to be repeated when there is snapshot to restore
            try {
                mc.writeJobExecutionRecordSafe(true);
            } catch (IndeterminateOperationStateException e) {
                // JobExecutionRecord is not safe, so we cannot restore from snapshot if it will be needed.
                // But even if there is no snapshot, the cluster probably is in unsafe state,
                // so it is better to wait with starting the job anyway.
                // Trigger a restart via exception, ordinary exception would cause job failure
                logger.warning("Job " + mc.jobName() +
                        " initial update of JobExecutionRecord was indeterminate." +
                        " Failed to start job. Will retry.");
                throw new JobTerminateRequestedException(RESTART_FORCEFUL);
            }

            if (terminationRequest != null) {
                if (terminationRequest.mode.actionAfterTerminate() != RESTART) {
                    throw new JobTerminateRequestedException(terminationRequest.mode);
                }
                // requested termination mode is RESTART, ignore it because we are just starting
                terminationRequest = null;
            }
            ClassLoader classLoader = mc.getJetServiceBackend().getJobClassLoaderService()
                                        .getOrCreateClassLoader(mc.jobConfig(), mc.jobId(), COORDINATOR);
            DAG dag;
            JobClassLoaderService jobClassLoaderService = mc.getJetServiceBackend().getJobClassLoaderService();
            try {
                jobClassLoaderService.prepareProcessorClassLoaders(mc.jobId());
                dag = deserializeWithCustomClassLoader(
                        mc.nodeEngine().getSerializationService(),
                        classLoader,
                        mc.jobRecord().getDag()
                );
            } catch (Exception e) {
                throw new JetException("DAG deserialization failed", e);
            } finally {
                jobClassLoaderService.clearProcessorClassLoaders();
            }
            // save a copy of the vertex list because it is going to change
            vertices = new HashSet<>();
            verticesCompleted = false;
            dag.iterator().forEachRemaining(vertices::add);
            mc.setExecutionId(mc.jobRepository().newExecutionId());
            mc.snapshotContext().onExecutionStarted();
            executionCompletionFuture = new CompletableFuture<>();
            return dag;
        } finally {
            mc.unlock();
        }
    }

    /**
     * Returns a tuple of: <ol>
     * <li> a future that will be completed when the execution completes (or
     *      a completed future, if execution is not RUNNING or STARTING)
     * <li> a string with a message why this call did nothing or null, if
     *      this call actually initiated the termination
     * </ol>
     *
     * @param allowWhileExportingSnapshot if false and jobStatus is
     *        SUSPENDED_EXPORTING_SNAPSHOT, termination will be rejected
     * @param userInitiated if the termination was requested by the user
     */
    @Nonnull
    Tuple2<CompletableFuture<Void>, String> requestTermination(
            TerminationMode mode,
            @SuppressWarnings("SameParameterValue") boolean allowWhileExportingSnapshot,
            boolean userInitiated
    ) {
        mc.coordinationService().assertOnCoordinatorThread();

        ActionAfterTerminate action = mode.actionAfterTerminate();
        if ((action == SUSPEND || action == RESTART) && !isJobSuspendable(mc.jobConfig())) {
            // We cancel the job if it is not allowed to be suspended.
            mode = CANCEL_FORCEFUL;
        }

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
            if (terminationRequest != null) {
                // don't report the cancellation of a cancelled job as an error
                String message = terminationRequest.mode == CANCEL_FORCEFUL && mode == CANCEL_FORCEFUL ? null
                        : "Job is already terminating in mode: " + terminationRequest.mode.name();
                return tuple2(executionCompletionFuture, message);
            }
            terminationRequest = new TerminationRequest(mode, userInitiated);
            // handle cancellation of a suspended job
            if (localStatus == SUSPENDED || localStatus == SUSPENDED_EXPORTING_SNAPSHOT) {
                mc.setJobStatus(FAILED, mode.actionAfterTerminate().description(), true);
                setFinalResult(createCancellationException());
            }
            if (mode.isWithTerminalSnapshot()) {
                mc.snapshotContext().enqueueSnapshot(null, true, null);
            }

            result = tuple2(executionCompletionFuture, null);
        } finally {
            mc.unlock();
        }

        if (localStatus == SUSPENDED || localStatus == SUSPENDED_EXPORTING_SNAPSHOT) {
            try {
                mc.coordinationService()
                        .completeJob(mc,
                                createCancellationException(),
                                System.currentTimeMillis(), userInitiated)
                        .get();
            } catch (Exception e) {
                throw rethrow(e);
            }
        } else {
            if (localStatus == RUNNING || localStatus == STARTING) {
                handleTermination(mode);
            }
        }

        return result;
    }

    private void rewriteDagWithSnapshotRestore(DAG dag, long snapshotId, String mapName, String snapshotName) {
        // snapshot map is not updated here, so it does not need to be
        // configured with failOnIndeterminateOperationState
        IMap<Object, Object> snapshotMap = mc.nodeEngine().getHazelcastInstance().getMap(mapName);
        long resolvedSnapshotId = validateSnapshot(
                snapshotId, snapshotMap, mc.jobIdString(), snapshotName);
        logger.info(String.format(
                "About to restore the state of %s from snapshot %d, mapName = %s",
                mc.jobIdString(), resolvedSnapshotId, mapName));
        List<Vertex> originalVertices = new ArrayList<>();
        dag.iterator().forEachRemaining(originalVertices::add);

        Map<String, Integer> vertexToOrdinal = new HashMap<>();
        Vertex readSnapshotVertex = dag.newVertex(SNAPSHOT_VERTEX_PREFIX + "read", readMapP(mapName));
        Vertex explodeVertex = dag.newVertex(SNAPSHOT_VERTEX_PREFIX + "explode",
                () -> new ExplodeSnapshotP(vertexToOrdinal, resolvedSnapshotId));
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
        scheduleRestart("Quorum is absent");
        return true;
    }

    private boolean scheduleRestartIfClusterIsNotSafe() {
        if (mc.coordinationService().shouldStartJobs()) {
            return false;
        }

        logger.fine("Rescheduling restart of '" + mc.jobName() + "': cluster is not safe");
        scheduleRestart("Cluster is not safe");
        return true;
    }

    private void scheduleRestart(String description) {
        mc.assertLockHeld();
        JobStatus jobStatus = mc.jobStatus();
        if (jobStatus != NOT_RUNNING && jobStatus != STARTING && jobStatus != RUNNING) {
            throw new IllegalStateException("Restart scheduled in an unexpected state: " + jobStatus);
        }
        if (jobStatus != NOT_RUNNING) {
            mc.setJobStatus(NOT_RUNNING, description, false);
        }
        mc.coordinationService().scheduleRestart(mc.jobId());
    }

    // Called as callback when all InitOperation invocations are done
    private void onInitStepCompleted(Collection<Map.Entry<MemberInfo, Object>> responses) {
        mc.coordinationService().submitToCoordinatorThread(() -> {
            Throwable error = getErrorFromResponses("Init", responses);
            JobStatus status = mc.jobStatus();
            if (error == null && status == STARTING) {
                invokeStartExecution();
            } else {
                cancelExecutionInvocations(mc.jobId(), mc.executionId(), null, () ->
                        onStartExecutionComplete(error != null ? error
                                : new IllegalStateException("Cannot execute " + mc.jobIdString() + ": status is " + status),
                                emptyList())
                );
            }
        });
    }

    // If a participant leaves or the execution fails in a participant locally, executions are cancelled
    // on the remaining participants and the callback is completed after all invocations return.
    private void invokeStartExecution() {
        logger.fine("Executing " + mc.jobIdString());

        long executionId = mc.executionId();
        mc.resetStartOperationResponses();
        executionFailureCallback = new ExecutionFailureCallback(executionId, mc.startOperationResponses());
        getTerminationRequest().ifPresent(request -> handleTermination(request.getMode()));

        boolean savingMetricsEnabled = mc.jobConfig().isStoreMetricsAfterJobCompletion();
        Function<ExecutionPlan, Operation> operationCtor =
                plan -> new StartExecutionOperation(mc.jobId(), executionId, savingMetricsEnabled);
        Consumer<Collection<Map.Entry<MemberInfo, Object>>> completionCallback =
                responses -> onStartExecutionComplete(getErrorFromResponses("Execution", responses),
                        responses);

        mc.setJobStatus(RUNNING);
        // There can be a snapshot enqueued while the job is starting, start it now if that's the case
        mc.snapshotContext().tryBeginSnapshot();
        mc.invokeOnParticipants(operationCtor, completionCallback, executionFailureCallback, false);

        if (mc.jobConfig().getProcessingGuarantee() != NONE) {
            mc.coordinationService().scheduleSnapshot(mc, executionId);
        }
    }

    void handleTermination(@Nonnull TerminationMode mode) {
        // this method can be called multiple times to handle the termination, it must
        // be safe against it (idempotent).
        if (mode.isWithTerminalSnapshot()) {
            mc.snapshotContext().tryBeginSnapshot();
        } else if (executionFailureCallback != null) {
            executionFailureCallback.cancelInvocations(mode);
        }
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
     * <li> Returns {@code null} if there is no failure
     * <li> Returns a {@link CancellationException} if the job is cancelled
     * <li> Returns a {@link JobTerminateRequestedException} if the current
     *      execution is stopped due to a requested termination, except for
     *      {@link TerminationMode#CANCEL_GRACEFUL} and
     *      {@link TerminationMode#CANCEL_FORCEFUL}, in which case
     *      {@link CancellationException} is returned.
     * <li> If there is at least one user failure, such as an exception in user
     *      code (restartable or not), then returns that failure.
     * <li> Otherwise, the failure is because a job participant has left the
     *      cluster. In that case, it returns {@code TopologyChangeException} so
     *      that the job will be restarted
     * </ul>
     */
    private Throwable getErrorFromResponses(String opName, Collection<Map.Entry<MemberInfo, Object>> responses) {
        if (isCancelled()) {
            logger.fine(mc.jobIdString() + " to be cancelled after " + opName);
            return createCancellationException();
        }

        Map<Boolean, List<Entry<Address, Object>>> grouped = responses.stream()
                .map(en -> entry(en.getKey().getAddress(), en.getValue()))
                .collect(partitioningBy(e1 -> e1.getValue() instanceof Throwable));

        int successfulMembersCount = grouped.getOrDefault(false, emptyList()).size();
        if (successfulMembersCount == mc.executionPlanMap().size()) {
            logger.fine(opName + " of " + mc.jobIdString() + " was successful");
            return null;
        }

        List<Entry<Address, Object>> failures = grouped.getOrDefault(true, emptyList());
        if (!failures.isEmpty()) {
            logger.fine(opName + " of " + mc.jobIdString() + " has failures: " + failures);
        }

        // Handle TerminatedWithSnapshotException. If only part of the members
        // threw it and others completed normally, the terminal snapshot will fail,
        // but we still handle it as if terminal snapshot was done. If there are
        // other exceptions, ignore this and handle the other exception.
        if (failures.stream().allMatch(entry -> entry.getValue() instanceof TerminatedWithSnapshotException)) {
            assert opName.equals("Execution") : "opName is '" + opName + "', expected 'Execution'";
            logger.fine(opName + " of " + mc.jobIdString() + " terminated after a terminal snapshot");
            TerminationMode mode = requestedTerminationMode().orElseThrow(() -> new AssertionError("mode is null"));
            assert mode.isWithTerminalSnapshot() : "mode=" + mode;
            return mode == CANCEL_GRACEFUL ? createCancellationException() : new JobTerminateRequestedException(mode);
        }

        // If all exceptions are of certain type, treat it as TopologyChangedException
        Map<Boolean, List<Entry<Address, Object>>> splitFailures = failures.stream()
                .collect(partitioningBy(
                        e -> e.getValue() instanceof CancellationException
                                || e.getValue() instanceof TerminatedWithSnapshotException
                                || isTopologyException((Throwable) e.getValue())));
        List<Entry<Address, Object>> topologyFailures = splitFailures.getOrDefault(true, emptyList());
        List<Entry<Address, Object>> otherFailures = splitFailures.getOrDefault(false, emptyList());

        if (!otherFailures.isEmpty()) {
            return (Throwable) otherFailures.get(0).getValue();
        } else {
            return new TopologyChangedException("Causes from members: " + topologyFailures);
        }
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

    private void onStartExecutionComplete(Throwable error, Collection<Entry<MemberInfo, Object>> responses) {
        JobStatus status = mc.jobStatus();
        if (status != STARTING && status != RUNNING) {
            logCannotComplete(error);
            error = new IllegalStateException("Job coordination failed");
        }

        setFinalExecutionMetrics(responses.stream()
                .filter(e -> e.getValue() instanceof RawJobMetrics)
                .map(e -> (RawJobMetrics) e.getValue())
                .collect(toList()));

        if (error instanceof JobTerminateRequestedException
                && ((JobTerminateRequestedException) error).mode().isWithTerminalSnapshot()) {
            Throwable finalError = error;
            // The terminal snapshot on members is always completed before replying to StartExecutionOp.
            // However, the response to snapshot operations can be processed after the response to
            // StartExecutionOp, so wait for that too.
            mc.snapshotContext().terminalSnapshotFuture()
                    // have to use Async version, the future is completed inside a synchronized block
                    .whenCompleteAsync(withTryCatch(logger, (r, e) -> finalizeExecution(finalError)));
        } else {
            if (error instanceof ExecutionNotFoundException) {
                // If the StartExecutionOperation didn't find the execution, it means that it was cancelled.
                final Throwable notFoundException = error;
                error = getTerminationRequest()
                        // This cancellation can be because the master cancelled it. If that's the case, convert the exception
                        // to JobTerminateRequestedException.
                        .map(request -> new JobTerminateRequestedException(request.getMode())
                                .initCause(notFoundException))
                        // The cancellation can also happen if some participant left and
                        // the target cancelled the execution locally in JobExecutionService.onMemberRemoved().
                        // We keep this (and possibly other) exceptions as they are
                        // and let the execution complete with failure.
                        .orElse(notFoundException);

            }
            finalizeExecution(error);
        }
    }

    void cancelExecutionInvocations(long jobId, long executionId, TerminationMode mode, Runnable callback) {
        mc.nodeEngine().getExecutionService().execute(ExecutionService.ASYNC_EXECUTOR, () ->
                mc.invokeOnParticipants(plan -> new TerminateExecutionOperation(jobId, executionId, mode),
                        responses -> {
                            if (responses.stream()
                                    .map(Entry::getValue)
                                    .filter(value -> !(value instanceof JetDisabledException))
                                    .anyMatch(Objects::nonNull)) {
                                // log errors
                                logger.severe(mc.jobIdString() + ": some TerminateExecutionOperation invocations " +
                                        "failed, execution might remain stuck: " + responses);
                            }
                            if (callback != null) {
                                callback.run();
                            }
                        }, null, true));
    }

    void finalizeExecution(@Nullable Throwable failure) {
        mc.coordinationService().submitToCoordinatorThread(() -> {
            mc.lock();
            JobStatus status = mc.jobStatus();
            if (status == COMPLETED || status == FAILED) {
                logIgnoredCompletion(failure, status);
            }
            mc.unlock();
          })
          .thenComposeAsync(r -> completeVertices(failure))
          .thenCompose(ignored -> mc.coordinationService().submitToCoordinatorThread(() -> {
            final Runnable nonSynchronizedAction;
            try {
                mc.lock();
                mc.getJetServiceBackend().getJobClassLoaderService().tryRemoveClassloadersForJob(mc.jobId(), COORDINATOR);

                ActionAfterTerminate terminationModeAction = failure instanceof JobTerminateRequestedException
                        ? ((JobTerminateRequestedException) failure).mode().actionAfterTerminate() : null;
                mc.snapshotContext().onExecutionTerminated();

                String description = requestedTerminationMode()
                        .map(mode -> mode.actionAfterTerminate().description())
                        .orElse(failure != null ? failure.toString() : null);
                boolean userRequested = isUserInitiatedTermination();

                // if restart was requested, restart immediately
                if (terminationModeAction == RESTART) {
                    mc.setJobStatus(NOT_RUNNING, description, userRequested);
                    nonSynchronizedAction = () -> mc.coordinationService().restartJob(mc.jobId());
                } else if (!isCancelled() && isRestartableException(failure) && mc.jobConfig().isAutoScaling()) {
                    // if restart is due to a failure, schedule a restart after a delay
                    scheduleRestart(description);
                    nonSynchronizedAction = NO_OP;
                } else if (terminationModeAction == SUSPEND
                        || isRestartableException(failure)
                        && !isCancelled()
                        && !mc.jobConfig().isAutoScaling()
                        && mc.jobConfig().getProcessingGuarantee() != NONE
                ) {
                    mc.setJobStatus(SUSPENDED, description, userRequested);
                    mc.jobExecutionRecord().setSuspended(null);
                    nonSynchronizedAction = () -> mc.writeJobExecutionRecord(false);
                } else if (failure != null
                        && !isCancelled()
                        && !isCancelledGracefully()
                        && mc.jobConfig().isSuspendOnFailure()
                ) {
                    mc.setJobStatus(SUSPENDED, description, userRequested);
                    mc.jobExecutionRecord().setSuspended("Execution failure:\n" +
                            ExceptionUtil.stackTraceToString(failure));
                    nonSynchronizedAction = () -> mc.writeJobExecutionRecord(false);
                } else {
                    long completionTime = System.currentTimeMillis();
                    boolean isSuccess = logExecutionSummary(failure, completionTime);
                    mc.setJobStatus(isSuccess ? COMPLETED : FAILED, description, userRequested);
                    if (failure instanceof LocalMemberResetException) {
                        logger.fine("Cancelling job " + mc.jobIdString() + " locally: member (local or remote) reset. " +
                                "We don't delete job metadata: job will restart on majority cluster");
                        setFinalResult(new CancellationException());
                    } else {
                        mc.coordinationService()
                          .completeJob(mc, failure, completionTime, isUserInitiatedTermination())
                          .whenComplete(withTryCatch(logger, (r, f) -> {
                              if (f != null) {
                                  logger.warning("Completion of " + mc.jobIdString() + " failed", f);
                              } else {
                                  setFinalResult(failure);
                              }
                          }));
                    }
                    nonSynchronizedAction = NO_OP;
                }
                // reset the state for the next execution
                terminationRequest = null;
                executionFailureCallback = null;
            } finally {
                mc.unlock();
            }
            executionCompletionFuture.complete(null);
            nonSynchronizedAction.run();
        }));
    }

    /**
     * @return True, if the job completed successfully.
     */
    private boolean logExecutionSummary(@Nullable Throwable failure, long completionTime) {
        if (failure == null) {
            logger.info(formatExecutionSummary("completed successfully", completionTime));
            return true;
        }
        if (failure instanceof CancellationException || failure instanceof JobTerminateRequestedException) {
            logger.info(formatExecutionSummary("got terminated, reason=" + failure, completionTime));
            return false;
        }
        if (failure instanceof JetDisabledException) {
            logger.severe(formatExecutionSummary("failed. This is probably " +
                    "because the Jet engine is not enabled on all cluster members. " +
                    "Please enable the Jet engine for ALL members in the cluster.", completionTime), failure);
            return false;
        }
        logger.severe(formatExecutionSummary("failed", completionTime), failure);
        return false;
    }

    private String formatExecutionSummary(String conclusion, long completionTime) {
        StringBuilder sb = new StringBuilder();
        sb.append("Execution of ").append(mc.jobIdString()).append(' ').append(conclusion);
        sb.append("\n\t").append("Start time: ").append(Util.toLocalDateTime(executionStartTime));
        sb.append("\n\t").append("Duration: ").append(formatJobDuration(completionTime - executionStartTime));
        if (finalExecutionMetrics.stream().noneMatch(rjm -> rjm.getBlob() != null)) {
            sb.append("\n\tTo see additional job metrics enable JobConfig.storeMetricsAfterJobCompletion");
        } else {
            JobMetrics jobMetrics = JobMetricsUtil.toJobMetrics(finalExecutionMetrics);

            Map<String, Long> receivedCounts = mergeByVertex(jobMetrics.get(MetricNames.RECEIVED_COUNT));
            Map<String, Long> emittedCounts = mergeByVertex(jobMetrics.get(MetricNames.EMITTED_COUNT));
            Map<String, Long> distributedBytesIn = mergeByVertex(jobMetrics.get(MetricNames.DISTRIBUTED_BYTES_IN));
            Map<String, Long> distributedBytesOut = mergeByVertex(jobMetrics.get(MetricNames.DISTRIBUTED_BYTES_OUT));

            sb.append("\n\tVertices:");
            for (Vertex vertex : vertices) {
                sb.append("\n\t\t").append(vertex.getName());
                sb.append(getValueForVertex("\n\t\t\t" + MetricNames.RECEIVED_COUNT, vertex, receivedCounts));
                sb.append(getValueForVertex("\n\t\t\t" + MetricNames.EMITTED_COUNT, vertex, emittedCounts));
                sb.append(getValueForVertex("\n\t\t\t" + MetricNames.DISTRIBUTED_BYTES_IN, vertex, distributedBytesIn));
                sb.append(getValueForVertex("\n\t\t\t" + MetricNames.DISTRIBUTED_BYTES_OUT, vertex, distributedBytesOut));
            }
        }
        return sb.toString();
    }

    private static Map<String, Long> mergeByVertex(List<Measurement> measurements) {
        return measurements.stream().collect(toMap(
                m -> m.tag(MetricTags.VERTEX),
                Measurement::value,
                Long::sum
        ));
    }

    private static String getValueForVertex(String label, Vertex vertex, Map<String, Long> values) {
        Long value = values.get(vertex.getName());
        return value == null ? "" : label + ": " + value;
    }

    private void logIgnoredCompletion(@Nullable Throwable failure, JobStatus status) {
        if (failure != null) {
            logger.severe("Ignoring failure completion of " + idToString(mc.jobId()) + " because status is " + status,
                    failure);
        } else {
            logger.severe("Ignoring completion of " + idToString(mc.jobId()) + " because status is " + status);
        }
    }

    private CompletableFuture<Void> completeVertices(@Nullable Throwable failure) {
        if (vertices != null && !verticesCompleted) {
            ExecutorService offloadExecutor =
                    mc.nodeEngine().getExecutionService().getExecutor(JOB_OFFLOADABLE_EXECUTOR);
            List<CompletableFuture<Void>> futures = new ArrayList<>(vertices.size());
            JobClassLoaderService classLoaderService = mc.getJetServiceBackend().getJobClassLoaderService();
            for (Vertex v : vertices) {
                ProcessorMetaSupplier processorMetaSupplier = v.getMetaSupplier();
                RunnableEx closeAction = () -> {
                    try {
                        ClassLoader processorCl = classLoaderService.getProcessorClassLoader(mc.jobId(), v.getName());
                        doWithClassLoader(
                                processorCl,
                                () -> processorMetaSupplier.close(failure)
                        );
                    } catch (Throwable e) {
                        logger.severe(mc.jobIdString()
                                + " encountered an exception in ProcessorMetaSupplier.close(), ignoring it", e);
                    }
                };
                Executor executor = processorMetaSupplier.closeIsCooperative() ? CALLER_RUNS : offloadExecutor;
                futures.add(runAsync(closeAction, executor));
            }
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .whenComplete((v, e) -> verticesCompleted = true);
        }
        return completedFuture(null);
    }

    void resumeJob() {
        mc.lock();
        try {
            if (mc.jobStatus() != SUSPENDED) {
                logger.info("Not resuming " + mc.jobIdString() + ": not " + SUSPENDED + ", but " + mc.jobStatus());
                return;
            }
            mc.setJobStatus(NOT_RUNNING, "Resume", true);
        } finally {
            mc.unlock();
        }
        logger.fine("Resuming job " + mc.jobName());
        tryStartJob();
    }

    private boolean hasParticipant(UUID uuid) {
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
    CompletableFuture<Void> onParticipantGracefulShutdown(UUID uuid) {
        return hasParticipant(uuid) ? gracefullyTerminateOrCancel() : completedFuture(null);
    }

    @Nonnull
    CompletableFuture<Void> gracefullyTerminateOrCancel() {
        CompletableFuture<CompletableFuture<Void>> future = mc.coordinationService().submitToCoordinatorThread(
                () -> requestTermination(RESTART_GRACEFUL, false, false).f0());
        return future.thenCompose(Function.identity());
    }

    /**
     * Checks if the job is running on all members and maybe restart it.
     * <p>
     * Returns {@code false}, if this method should be scheduled to
     * be called later. That is, when the job is running, but we've
     * failed to request the restart.
     * <p>
     * Returns {@code true}, if the job is not running, has
     * auto-scaling disabled, is already running on all members or if
     * we've managed to request a restart.
     */
    boolean maybeScaleUp(int dataMembersWithPartitionsCount) {
        mc.coordinationService().assertOnCoordinatorThread();
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
        if (localStatus == RUNNING && requestTermination(TerminationMode.RESTART_GRACEFUL, false, false).f1() == null) {
            logger.info("Requested restart of " + mc.jobIdString() + " to make use of added member(s). "
                    + "Job was running on " + mc.executionPlanMap().size() + " members, cluster now has "
                    + dataMembersWithPartitionsCount + " data members with assigned partitions");
            return true;
        }

        // if status was not RUNNING or requestTermination didn't succeed, we'll try again later.
        return false;
    }

    /**
     * Returns aggregated job and execution metrics.
     *
     * @implNote
     * {@linkplain JobCoordinationService#completeJob When terminal metrics are requested},
     * job and execution metrics must have already been collected, for which there are two
     * code paths. In both cases, the same coordinator thread follows the given flow before
     * requesting the terminal metrics. <ol type=a>
     * <li> If the job is not suspended before termination: <ol>
     *      <li> {@link JobExecutionService#beginExecution0}: Collection of terminal execution
     *           metrics is triggered.
     *      <li> {@link #onStartExecutionComplete} → <ol type=i>
     *           <li> {@link #setFinalExecutionMetrics}: Terminal execution metrics are saved.
     *           <li> {@link #finalizeExecution} → {@link MasterContext#setJobStatus} →
     *                {@link #setJobMetrics}: Terminal job metrics are saved. </ol></ol>
     * <li> If the job is suspended before termination: <ol>
     *      <li> Terminal execution metrics are already collected when suspending the job.
     *      <li> {@link #requestTermination} → {@link MasterContext#setJobStatus} →
     *           {@link #setJobMetrics}: Terminal job metrics are saved. </ol></ol>
     *
     * @see MasterContext#provideDynamicMetrics
     */
    List<RawJobMetrics> persistentMetrics() {
        if (!mc.metricsEnabled()) {
            return emptyList();
        }
        return withJobMetrics(finalExecutionMetrics);
    }

    /**
     * Enriches execution metrics with job metrics if any.
     */
    @Nonnull
    private List<RawJobMetrics> withJobMetrics(@Nonnull List<RawJobMetrics> executionMetrics) {
        List<RawJobMetrics> metrics = new ArrayList<>(executionMetrics.size() + 1);
        if (jobMetrics != null) {
            // It is initialized in the constructor, but due to instruction reordering, it can be null.
            metrics.add(jobMetrics);
        }
        metrics.addAll(executionMetrics);
        return metrics;
    }

    /**
     * Generates persistent job metrics, namely {@link MetricNames#JOB_STATUS} and
     * {@link MetricNames#IS_USER_CANCELLED}.
     *
     * @see MasterContext#provideDynamicMetrics
     */
    void setJobMetrics(JobStatus status, boolean userRequested) {
        if (!mc.metricsEnabled()) {
            return;
        }
        MetricsCompressor compressor = new MetricsCompressor();
        compressor.addLong(statusMetricDescriptor, status.getId());
        boolean isUserCancelled = status == FAILED && userRequested;
        compressor.addLong(userCancelledMetricDescriptor, isUserCancelled ? 1 : 0);
        jobMetrics = RawJobMetrics.of(compressor.getBlobAndClose());
    }

    private void setFinalExecutionMetrics(@Nonnull List<RawJobMetrics> executionMetrics) {
        if (!mc.metricsEnabled()) {
            return;
        }
        assert executionMetrics.stream().allMatch(Objects::nonNull) : "responses=" + executionMetrics;
        finalExecutionMetrics = executionMetrics;
    }

    void collectMetrics(CompletableFuture<List<RawJobMetrics>> clientFuture) {
        if (mc.jobStatus() == RUNNING) {
            long jobId = mc.jobId();
            long executionId = mc.executionId();
            mc.invokeOnParticipants(
                    plan -> new GetLocalExecutionMetricsOperation(jobId, executionId),
                    objects -> completeWithMetrics(clientFuture, objects),
                    null,
                    false
            );
        } else {
            clientFuture.complete(persistentMetrics());
        }
    }

    private void completeWithMetrics(CompletableFuture<List<RawJobMetrics>> clientFuture,
                                     Collection<Map.Entry<MemberInfo, Object>> metrics) {
        if (metrics.stream().anyMatch(en -> en.getValue() instanceof ExecutionNotFoundException)) {
            // If any member threw ExecutionNotFoundException, we'll retry. This happens
            // when the job is starting or completing - master sees the job as
            // RUNNING, but some members might have terminated already. When
            // retrying, the job will eventually not be RUNNING, in which case
            // we'll return last known metrics, or it will be running again, in
            // which case we'll get fresh metrics.
            logFinest(logger, "Rescheduling collectMetrics for %s, some members threw %s", mc.jobIdString(),
                    ExecutionNotFoundException.class.getSimpleName());
            mc.nodeEngine().getExecutionService().schedule(() ->
                    collectMetrics(clientFuture), COLLECT_METRICS_RETRY_DELAY_MILLIS, MILLISECONDS);
            return;
        }
        Throwable firstThrowable = (Throwable) metrics.stream().map(Map.Entry::getValue)
                                                      .filter(Throwable.class::isInstance).findFirst().orElse(null);
        if (firstThrowable != null) {
            clientFuture.completeExceptionally(firstThrowable);
        } else {
            clientFuture.complete(withJobMetrics(toList(metrics, e -> (RawJobMetrics) e.getValue())));
        }
    }

    /**
     * Specific type of edge to be used when restoring snapshots
     */
    public static class SnapshotRestoreEdge extends Edge {

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

    public static class TerminationRequest {
        /**
         * Requested termination mode
         */
        private final TerminationMode mode;
        /**
         * If the termination was initiated by the user.
         * <p>
         * Note that at present this information is trustworthy only for
         * cancellations. For other modes of termination we may not have enough
         * information about what initiated it. In dubious cases we default to false
         * i.e. not user-initiated action.
         */
        private final boolean userInitiated;

        public TerminationRequest(@Nonnull TerminationMode mode, boolean userInitiated) {
            this.mode = mode;
            this.userInitiated = userInitiated;
        }

        TerminationMode getMode() {
            return mode;
        }

        boolean isUserInitiated() {
            return userInitiated;
        }
    }

    /**
     * Attached to {@link StartExecutionOperation} invocations to cancel
     * invocations in case of a failure.
     */
    private class ExecutionFailureCallback implements BiConsumer<Address, Object> {

        private final AtomicBoolean invocationsCancelled = new AtomicBoolean();
        private final long executionId;
        private final Map<Address, CompletableFuture<Void>> startOperationResponses;

        ExecutionFailureCallback(long executionId, Map<Address, CompletableFuture<Void>> startOperationResponses) {
            this.executionId = executionId;
            this.startOperationResponses = startOperationResponses;
        }

        @Override
        public void accept(Address address, Object response) {
            LoggingUtil.logFine(logger, "%s received response to StartExecutionOperation from %s: %s",
                    mc.jobIdString(), address, response);
            CompletableFuture<Void> future = startOperationResponses.get(address);
            if (response instanceof Throwable) {
                Throwable throwable = (Throwable) response;
                future.completeExceptionally(throwable);

                if (!(peel(throwable) instanceof TerminatedWithSnapshotException)) {
                    cancelInvocations(null);
                }
            } else {
                // complete successfully
                future.complete(null);
            }
        }

        void cancelInvocations(TerminationMode mode) {
            if (invocationsCancelled.compareAndSet(false, true)) {
                cancelExecutionInvocations(mc.jobId(), executionId, mode, null);
            }
        }
    }
}
