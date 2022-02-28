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

package com.hazelcast.jet.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembershipManager;
import com.hazelcast.internal.cluster.impl.operations.TriggerMemberListPublishOp;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.internal.metrics.impl.MetricsCompressor;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.impl.deployment.JetDelegatingClassLoader;
import com.hazelcast.jet.impl.exception.ExecutionNotFoundException;
import com.hazelcast.jet.impl.exception.JobTerminateRequestedException;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.ExecutionContext.SenderReceiverKey;
import com.hazelcast.jet.impl.execution.SenderTasklet;
import com.hazelcast.jet.impl.execution.TaskletExecutionService;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.metrics.RawJobMetrics;
import com.hazelcast.jet.impl.operation.CheckLightJobsOperation;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.JobClassLoaderService.JobPhase.EXECUTION;
import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.doWithClassLoader;
import static com.hazelcast.jet.impl.util.Util.jobIdAndExecutionId;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;

/**
 * Service to handle ExecutionContexts on all cluster members. Job-control
 * operations from coordinator are handled here.
 */
public class JobExecutionService implements DynamicMetricsProvider {

    /**
     * A timeout after which we cancel a light job that doesn't receive InitOp
     * from the coordinator. {@link ExecutionContext} can be created in
     * response to data packet received for that execution, but it doesn't know
     * the coordinator. Therefore, the checker cannot confirm with the
     * coordinator if it still exists. We terminate these jobs after a timeout.
     * However, the timeout has to be long enough because if the job happens to
     * be initialized later, we'll lose data, and we won't even detect it. It can
     * also happen that we lose a DONE_ITEM and the job will get stuck, though
     * that's better than incorrect results.
     */
    private static final long UNINITIALIZED_CONTEXT_MAX_AGE_NS = MINUTES.toNanos(5);

    private static final long FAILED_EXECUTION_EXPIRY_NS = SECONDS.toNanos(5);

    private final Object mutex = new Object();

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final TaskletExecutionService taskletExecutionService;
    private final JobClassLoaderService jobClassloaderService;

    private final Set<Long> executionContextJobIds = newSetFromMap(new ConcurrentHashMap<>());

    // key: executionId
    private final ConcurrentMap<Long, ExecutionContext> executionContexts = new ConcurrentHashMap<>();

    /**
     * Key: executionId
     * Value: expiry time (as per System.nanoTime())
     * <p>
     * This map contains executions, that failed or were cancelled.
     * These executions are very likely to receive further data packets
     * from other members whose executions are concurrently cancelled
     * too. If we keep no track of these exceptions, in failure-heavy or
     * cancellation-heavy scenarios a significant amount of memory could
     * be held for time defined in {@link
     * #UNINITIALIZED_CONTEXT_MAX_AGE_NS}, see
     * https://github.com/hazelcast/hazelcast/issues/19897.
     */
    private final ConcurrentMap<Long, Long> failedJobs = new ConcurrentHashMap<>();

    @Probe(name = MetricNames.JOB_EXECUTIONS_STARTED)
    private final Counter executionStarted = MwCounter.newMwCounter();
    @Probe(name = MetricNames.JOB_EXECUTIONS_COMPLETED)
    private final Counter executionCompleted = MwCounter.newMwCounter();

    private final Function<? super Long, ? extends ExecutionContext> newLightJobExecutionContextFunction;

    private final ScheduledFuture<?> lightExecutionsCheckerFuture;

    JobExecutionService(NodeEngineImpl nodeEngine, TaskletExecutionService taskletExecutionService,
                        JobClassLoaderService jobClassloaderService) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.taskletExecutionService = taskletExecutionService;
        this.jobClassloaderService = jobClassloaderService;

        newLightJobExecutionContextFunction = execId ->
                failedJobs.containsKey(execId)
                        ? null
                        : new ExecutionContext(nodeEngine, execId, execId, true);

        // register metrics
        MetricsRegistry registry = nodeEngine.getMetricsRegistry();
        MetricDescriptor descriptor = registry.newMetricDescriptor()
                .withTag(MetricTags.MODULE, "jet");
        registry.registerStaticMetrics(descriptor, this);

        this.lightExecutionsCheckerFuture = nodeEngine.getExecutionService().scheduleWithRepetition(
                this::checkExecutions, 0, 1, SECONDS);
    }

    public Long getExecutionIdForJobId(long jobId) {
        return executionContexts.values().stream()
                                .filter(ec -> ec.jobId() == jobId)
                                .findAny()
                                .map(ExecutionContext::executionId)
                                .orElse(null);
    }

    public ExecutionContext getExecutionContext(long executionId) {
        return executionContexts.get(executionId);
    }

    /**
     * Gets the execution context or creates it, if it doesn't exist. If
     * we're creating it, we assume it's for a light job and that the
     * jobId == executionId. Might return null if the job with the given
     * ID recently failed.
     * <p>
     * We can also end up here for a non-light job in this scenario:<ul>
     *     <li>job runs on 2 members. The master requests termination.
     *     <li>execution on member A terminates and is removed from
     *         executionContexts
     *     <li>member A receives a packet from member B (because it was in transit
     *         or simply because the execution on member B might terminate a little
     *         later)
     *     <li>ExecutionContext is recreated.
     * </ul>
     *
     * We ignore this as we assume that we'll never receive the
     * StartExecutionOperation. The improperly-created ExecutionContext will be
     * removed after a timeout in {@link #checkExecutions()} because it
     * will never be initialized.
     * <p>
     * We mitigate the number of execution context created after a job
     * failed by checking the {@link #failedJobs} map before re-creating
     * the execution context in this method.
     */
    @Nullable
    public ExecutionContext getOrCreateExecutionContext(long executionId) {
        return executionContexts.computeIfAbsent(executionId, newLightJobExecutionContextFunction);
    }

    public Collection<ExecutionContext> getExecutionContexts() {
        return executionContexts.values();
    }

    public ConcurrentMap<Long, Long> getFailedJobs() {
        return failedJobs;
    }

    Map<SenderReceiverKey, SenderTasklet> getSenderMap(long executionId) {
        ExecutionContext ctx = executionContexts.get(executionId);
        return ctx != null ? ctx.senderMap() : null;
    }

    public void shutdown() {
        lightExecutionsCheckerFuture.cancel(false);
        synchronized (mutex) {
            cancelAllExecutions("Node is shutting down");
        }
    }

    public void reset() {
        cancelAllExecutions("reset");
    }

    /**
     * Cancels all ongoing executions using the given failure supplier.
     */
    public void cancelAllExecutions(String reason) {
        for (ExecutionContext exeCtx : executionContexts.values()) {
            LoggingUtil.logFine(logger, "Completing %s locally. Reason: %s",
                    exeCtx.jobNameAndExecutionId(), reason);
            terminateExecution0(exeCtx, null, new CancellationException());
        }
    }

    /**
     * Cancels executions that contain the leaving address as the coordinator or a
     * job participant
     */
    void onMemberRemoved(Member member) {
        Address address = member.getAddress();
        executionContexts.values().stream()
             // note that coordinator might not be a participant (in case it is a lite member)
             .filter(exeCtx -> exeCtx.coordinator() != null
                     && (exeCtx.coordinator().equals(address) || exeCtx.hasParticipant(address)))
             .forEach(exeCtx -> {
                 LoggingUtil.logFine(logger, "Completing %s locally. Reason: Member %s left the cluster",
                         exeCtx.jobNameAndExecutionId(), address);
                 terminateExecution0(exeCtx, null, new MemberLeftException(member));
             });
    }

    public CompletableFuture<RawJobMetrics> runLightJob(
            long jobId,
            long executionId,
            Address coordinator,
            int coordinatorMemberListVersion,
            Set<MemberInfo> participants,
            ExecutionPlan plan
    ) {
        assert executionId == jobId : "executionId(" + idToString(executionId) + ") != jobId(" + idToString(jobId) + ")";
        verifyClusterInformation(jobId, executionId, coordinator, coordinatorMemberListVersion, participants);
        failIfNotRunning();

        ExecutionContext execCtx;
        synchronized (mutex) {
            addExecutionContextJobId(jobId, executionId, coordinator);
            execCtx = executionContexts.computeIfAbsent(executionId,
                    x -> new ExecutionContext(nodeEngine, jobId, executionId, true));
        }

        try {
            Set<Address> addresses = participants.stream().map(MemberInfo::getAddress).collect(toSet());
            ClassLoader jobCl = jobClassloaderService.getClassLoader(jobId);
            // We don't create the CL for light jobs.
            assert jobClassloaderService.getClassLoader(jobId) == null;
            doWithClassLoader(
                    jobCl,
                    () -> execCtx.initialize(coordinator, addresses, plan)
            );
        } catch (Throwable e) {
            completeExecution(execCtx, new CancellationException());
            throw e;
        }

        // initial log entry with all of jobId, jobName, executionId
        if (logger.isFineEnabled()) {
            logger.fine("Execution plan for light job ID=" + idToString(jobId)
                    + ", jobName=" + (execCtx.jobName() != null ? '\'' + execCtx.jobName() + '\'' : "null")
                    + ", executionId=" + idToString(executionId) + " initialized, will start the execution");
        }

        return beginExecution0(execCtx, false);
    }

    /**
     * Initiates the given execution if the local node accepts the coordinator
     * as its master, and has an up-to-date member list information.
     * <ul><li>
     *     If the local node has a stale member list, it retries the init operation
     *     until it receives the new member list from the master.
     * </li><li>
     *     If the local node detects that the member list changed after the init
     *     operation is sent but before executed, then it sends a graceful failure
     *     so that the job init will be retried properly.
     * </li><li>
     *     If there is an already ongoing execution for the given job, then the
     *     init execution is retried.
     * </li></ul>
     */
    public void initExecution(
            long jobId, long executionId, Address coordinator, int coordinatorMemberListVersion,
            Set<MemberInfo> participants, ExecutionPlan plan
    ) {
        ExecutionContext execCtx = addExecutionContext(
                jobId, executionId, coordinator, coordinatorMemberListVersion, participants);

        try {
            jobClassloaderService.prepareProcessorClassLoaders(jobId);
            Set<Address> addresses = participants.stream().map(MemberInfo::getAddress).collect(toSet());
            ClassLoader jobCl = jobClassloaderService.getClassLoader(jobId);
            doWithClassLoader(jobCl, () -> execCtx.initialize(coordinator, addresses, plan));
        } finally {
            jobClassloaderService.clearProcessorClassLoaders();
        }


        // initial log entry with all of jobId, jobName, executionId
        logger.info("Execution plan for jobId=" + idToString(jobId)
                + ", jobName=" + (execCtx.jobName() != null ? '\'' + execCtx.jobName() + '\'' : "null")
                + ", executionId=" + idToString(executionId) + " initialized");
    }

    private void addExecutionContextJobId(long jobId, long executionId, Address coordinator) {
        if (!executionContextJobIds.add(jobId)) {
            ExecutionContext current = executionContexts.get(executionId);
            if (current != null) {
                throw new IllegalStateException(String.format(
                        "Execution context for %s for coordinator %s already exists for coordinator %s",
                        current.jobNameAndExecutionId(), coordinator, current.coordinator()));
            }

            // search contexts for one with different executionId, but same jobId
            if (logger.isFineEnabled()) {
                executionContexts.values().stream()
                                 .filter(e -> e.jobId() == jobId)
                                 .forEach(e -> logger.fine(String.format(
                                         "Execution context for job %s for coordinator %s already exists"
                                                 + " with local execution %s for coordinator %s",
                                         idToString(jobId), coordinator, idToString(e.executionId()),
                                         e.coordinator())));
            }

            throw new RetryableHazelcastException();
        }
    }

    private ExecutionContext addExecutionContext(
            long jobId,
            long executionId,
            Address coordinator,
            int coordinatorMemberListVersion,
            Set<MemberInfo> participants
    ) {
        ExecutionContext execCtx;
        ExecutionContext oldContext;
        try {
            assertIsMaster(jobId, executionId, coordinator);
            verifyClusterInformation(jobId, executionId, coordinator, coordinatorMemberListVersion, participants);
            failIfNotRunning();

            synchronized (mutex) {
                addExecutionContextJobId(jobId, executionId, coordinator);
                execCtx = new ExecutionContext(nodeEngine, jobId, executionId, false);
                oldContext = executionContexts.put(executionId, execCtx);
            }
        } catch (Throwable t) {
            // The classloader was created in InitExecutionOperation#deserializePlan().
            // If the InitExecutionOperation#doRun() fails before ExecutionContext is added
            // to executionContexts, then classloader must be removed in order to not have leaks.
            jobClassloaderService.tryRemoveClassloadersForJob(jobId, EXECUTION);
            throw t;
        }
        if (oldContext != null) {
            throw new RuntimeException("Duplicate ExecutionContext for execution " + Util.idToString(executionId));
        }
        return execCtx;
    }

    private void assertIsMaster(long jobId, long executionId, Address coordinator) {
        Address masterAddress = nodeEngine.getMasterAddress();
        if (!coordinator.equals(masterAddress)) {
            failIfNotRunning();

            throw new IllegalStateException(String.format(
                    "Coordinator %s cannot initialize %s. Reason: it is not the master, the master is %s",
                    coordinator, jobIdAndExecutionId(jobId, executionId), masterAddress));
        }
    }

    private void verifyClusterInformation(long jobId, long executionId, Address coordinator,
                                          int coordinatorMemberListVersion, Set<MemberInfo> participants) {
        Address masterAddress = nodeEngine.getMasterAddress();
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
        MembershipManager membershipManager = clusterService.getMembershipManager();
        int localMemberListVersion = membershipManager.getMemberListVersion();
        Address thisAddress = nodeEngine.getThisAddress();

        if (coordinatorMemberListVersion > localMemberListVersion) {
            if (masterAddress == null) {
                // we expect that master will eventually be known to this member (a new master will be
                // elected or split brain merge will happen).
                throw new RetryableHazelcastException(String.format(
                        "Cannot initialize %s for coordinator %s, local member list version %s," +
                                " coordinator member list version %s. And also, since the master address" +
                                " is not known to this member, cannot request a new member list from master.",
                        jobIdAndExecutionId(jobId, executionId), coordinator, localMemberListVersion,
                        coordinatorMemberListVersion));
            }
            assert !masterAddress.equals(thisAddress) : String.format(
                    "Local node: %s is master but InitOperation has coordinator member list version: %s larger than "
                    + " local member list version: %s", thisAddress, coordinatorMemberListVersion,
                    localMemberListVersion);

            nodeEngine.getOperationService().send(new TriggerMemberListPublishOp(), masterAddress);
            throw new RetryableHazelcastException(String.format(
                    "Cannot initialize %s for coordinator %s, local member list version %s," +
                            " coordinator member list version %s",
                    jobIdAndExecutionId(jobId, executionId), coordinator, localMemberListVersion,
                    coordinatorMemberListVersion));
        }
        // If the participant members can receive the new member list before the
        // coordinator, and we can also get into the
        // "coordinatorMemberListVersion < localMemberListVersion" case. If this
        // situation occurs when a job participant leaves, then the job start will
        // fail. Since the unknown participating member situation couldn't
        // be resolved with retrying the InitExecutionOperation for this
        // case, we do nothing here and let it fail below if some participant
        // isn't found.
        // The job start won't fail if this situation occurs when a new member
        // is added to the cluster, because all job participants are known to the
        // other participating members. The only disadvantage of this is that a
        // newly added member will not be a job participant and partition mapping
        // may not be completely proper in this case.

        boolean isLocalMemberParticipant = false;
        for (MemberInfo participant : participants) {
            if (participant.getAddress().equals(thisAddress)) {
                isLocalMemberParticipant = true;
            }

            if (membershipManager.getMember(participant.getAddress(), participant.getUuid()) == null) {
                throw new TopologyChangedException(String.format(
                        "Cannot initialize %s for coordinator %s: participant %s not found in local member list." +
                                " Local member list version: %s, coordinator member list version: %s",
                        jobIdAndExecutionId(jobId, executionId), coordinator, participant,
                        localMemberListVersion, coordinatorMemberListVersion));
            }
        }

        if (!isLocalMemberParticipant) {
            throw new IllegalArgumentException(String.format(
                    "Cannot initialize %s since member %s is not in participants: %s",
                    jobIdAndExecutionId(jobId, executionId), thisAddress, participants));
        }
    }

    private void failIfNotRunning() {
        if (!nodeEngine.isRunning()) {
            throw new HazelcastInstanceNotActiveException();
        }
    }

    @Nonnull
    public ExecutionContext assertExecutionContext(Address callerAddress, long jobId, long executionId,
                                                   String callerOpName) {
        Address masterAddress = nodeEngine.getMasterAddress();
        if (!callerAddress.equals(masterAddress)) {
            failIfNotRunning();

            throw new IllegalStateException(String.format(
                    "Caller %s cannot do '%s' for %s: it is not the master, the master is %s",
                    callerAddress, callerOpName, jobIdAndExecutionId(jobId, executionId), masterAddress));
        }

        failIfNotRunning();

        ExecutionContext executionContext = executionContexts.get(executionId);
        if (executionContext == null) {
            throw new ExecutionNotFoundException(String.format(
                    "%s not found for coordinator %s for '%s'",
                    jobIdAndExecutionId(jobId, executionId), callerAddress, callerOpName));
        } else if (!(executionContext.coordinator().equals(callerAddress) && executionContext.jobId() == jobId)) {
            throw new IllegalStateException(String.format(
                    "%s, originally from coordinator %s, cannot do '%s' by coordinator %s and execution %s",
                    executionContext.jobNameAndExecutionId(), executionContext.coordinator(),
                    callerOpName, callerAddress, idToString(executionId)));
        }

        return executionContext;
    }

    /**
     * Completes and cleans up execution of the given job
     */
    public void completeExecution(@Nonnull ExecutionContext executionContext, Throwable error) {
        ExecutionContext removed = executionContexts.remove(executionContext.executionId());
        if (removed != null) {
            if (error != null) {
                failedJobs.put(executionContext.executionId(), System.nanoTime() + FAILED_EXECUTION_EXPIRY_NS);
            }
            JetDelegatingClassLoader jobClassLoader = jobClassloaderService.getClassLoader(executionContext.jobId());
            try {
                doWithClassLoader(jobClassLoader, () -> executionContext.completeExecution(error));
            } finally {
                if (!executionContext.isLightJob()) {
                    jobClassloaderService.tryRemoveClassloadersForJob(executionContext.jobId(), EXECUTION);
                }
                executionCompleted.inc();
                executionContextJobIds.remove(executionContext.jobId());
                logger.fine("Completed execution of " + executionContext.jobNameAndExecutionId());
            }
        }
    }

    public void updateMetrics(@Nonnull Long executionId, RawJobMetrics metrics) {
        ExecutionContext executionContext = executionContexts.get(executionId);
        if (executionContext != null) {
            executionContext.setJobMetrics(metrics);
        }
    }

    public CompletableFuture<RawJobMetrics> beginExecution(
            Address coordinator,
            long jobId,
            long executionId,
            boolean collectMetrics
    ) {
        ExecutionContext execCtx = assertExecutionContext(coordinator, jobId, executionId, "StartExecutionOperation");
        assert !execCtx.isLightJob() : "StartExecutionOperation received for a light job " + idToString(jobId);
        logger.info("Start execution of " + execCtx.jobNameAndExecutionId() + " from coordinator " + coordinator);
        return beginExecution0(execCtx, collectMetrics);
    }

    public CompletableFuture<RawJobMetrics> beginExecution0(ExecutionContext execCtx, boolean collectMetrics) {
        executionStarted.inc();
        return execCtx.beginExecution(taskletExecutionService)
                .thenApply(r -> {
                    RawJobMetrics terminalMetrics;
                    if (collectMetrics) {
                        JobMetricsCollector metricsRenderer =
                                new JobMetricsCollector(execCtx.executionId(), nodeEngine.getLocalMember(), logger);
                        nodeEngine.getMetricsRegistry().collect(metricsRenderer);
                        terminalMetrics = metricsRenderer.getMetrics();
                    } else {
                        terminalMetrics = null;
                    }
                    return terminalMetrics;
                })
                .whenCompleteAsync(withTryCatch(logger, (i, e) -> {
                    completeExecution(execCtx, peel(e));

                    if (e instanceof CancellationException) {
                        logger.fine("Execution of " + execCtx.jobNameAndExecutionId() + " was cancelled");
                    } else if (e != null) {
                        logger.fine("Execution of " + execCtx.jobNameAndExecutionId()
                                + " completed with failure", e);
                    } else {
                        logger.fine("Execution of " + execCtx.jobNameAndExecutionId() + " completed");
                    }
                }));
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        try {
            descriptor.withTag(MetricTags.MODULE, "jet");
            executionContexts.forEach((id, ctx) ->
                ctx.provideDynamicMetrics(descriptor.copy(), context));
        } catch (Throwable t) {
            logger.warning("Dynamic metric collection failed", t);
            throw t;
        }
    }

    /**
     * See also javadoc at {@link CheckLightJobsOperation}.
     */
    private void checkExecutions() {
        try {
            long now = System.nanoTime();
            long uninitializedContextThreshold = now - UNINITIALIZED_CONTEXT_MAX_AGE_NS;
            Map<Address, List<Long>> executionsPerMember = new HashMap<>();

            for (ExecutionContext ctx : executionContexts.values()) {
                if (!ctx.isLightJob()) {
                    continue;
                }
                Address coordinator = ctx.coordinator();
                if (coordinator != null) {
                    // if coordinator is known, add execution to the list to check
                    executionsPerMember
                            .computeIfAbsent(coordinator, k -> new ArrayList<>())
                            .add(ctx.executionId());
                } else {
                    // if coordinator is not known, remove execution if it's not known for too long
                    if (ctx.getCreatedOn() <= uninitializedContextThreshold) {
                        LoggingUtil.logFine(logger, "Terminating light job %s because it wasn't initialized during %d seconds",
                                idToString(ctx.executionId()), NANOSECONDS.toSeconds(UNINITIALIZED_CONTEXT_MAX_AGE_NS));
                        terminateExecution0(ctx, TerminationMode.CANCEL_FORCEFUL, new CancellationException());
                    }
                }
            }

            // submit the query to the coordinator
            for (Entry<Address, List<Long>> en : executionsPerMember.entrySet()) {
                long[] executionIds = en.getValue().stream().mapToLong(Long::longValue).toArray();
                Operation op = new CheckLightJobsOperation(executionIds);
                InvocationFuture<long[]> future = nodeEngine.getOperationService()
                        .createInvocationBuilder(JetServiceBackend.SERVICE_NAME, op, en.getKey())
                        .invoke();
                future.whenComplete((r, t) -> {
                    if (t instanceof TargetNotMemberException) {
                        // if the target isn't a member, then all executions are unknown
                        r = executionIds;
                    } else if (t != null) {
                        logger.warning("Failed to check light job state with coordinator " + en.getKey() + ": " + t, t);
                        return;
                    }
                    assert r != null;
                    for (long executionId : r) {
                        ExecutionContext execCtx = executionContexts.get(executionId);
                        if (execCtx != null) {
                            logger.fine("Terminating light job " + idToString(executionId)
                                    + " because the coordinator doesn't know it");
                            terminateExecution0(execCtx, TerminationMode.CANCEL_FORCEFUL, new CancellationException());
                        }
                    }
                });
            }

            // clean up failedJobs
            failedJobs.values().removeIf(expiryTime -> expiryTime < now);
        } catch (Throwable e) {
            logger.severe("Failed to query live light executions: " + e, e);
        }
    }

    public void terminateExecution(long jobId, long executionId, Address callerAddress, TerminationMode mode) {
        failIfNotRunning();

        ExecutionContext executionContext = executionContexts.get(executionId);
        if (executionContext == null) {
            // If this happens after the execution terminated locally, ignore.
            // If this happens before the execution was initialized locally, that means it's a light
            // job. We ignore too and rely on the CheckLightJobsOperation.
            return;
        }
        if (!executionContext.isLightJob()) {
            Address masterAddress = nodeEngine.getMasterAddress();
            if (!callerAddress.equals(masterAddress)) {
                failIfNotRunning();

                throw new IllegalStateException(String.format(
                        "Caller %s cannot do '%s' for terminateExecution: it is not the master, the master is %s",
                        callerAddress, jobIdAndExecutionId(jobId, executionId), masterAddress));
            }
        }
        Address coordinator = executionContext.coordinator();
        if (coordinator == null) {
            // This can happen if ExecutionContext was created after a received data packet,
            // either before the initialization or after a completion.
            // The TerminateOp is always sent after InitOp on coordinator, but it can happen that it's handled
            // first on the target member.
            // We ignore this and rely on the CheckLightJobsOperation to clean up.
            // It can't happen for normal jobs
            assert executionContext.isLightJob() : "null coordinator for non-light job";
        } else if (!coordinator.equals(callerAddress)) {
            throw new IllegalStateException(String.format(
                    "%s, originally from coordinator %s, cannot do 'terminateExecution' by coordinator %s and execution %s",
                    executionContext.jobNameAndExecutionId(), coordinator, callerAddress, idToString(executionId)));
        }
        Exception cause = mode == null ? new CancellationException() : new JobTerminateRequestedException(mode);
        terminateExecution0(executionContext, mode, cause);
    }

    public void terminateExecution0(ExecutionContext executionContext, TerminationMode mode, Throwable cause) {
        if (!executionContext.terminateExecution(mode)) {
            // If the execution was terminated before it began, call completeExecution now.
            // Otherwise, if the execution was already begun, this method will be called when the tasklets complete.
            logger.fine(executionContext.jobNameAndExecutionId()
                    + " calling completeExecution because execution terminated before it started");
            completeExecution(executionContext, cause);
        }
    }

    // for test
    public void waitAllExecutionsTerminated() {
        for (ExecutionContext ctx : executionContexts.values()) {
            try {
                ctx.getExecutionFuture().join();
            } catch (Throwable ignored) {
            }
        }
    }

    private static class JobMetricsCollector implements MetricsCollector {

        private final Long executionId;
        private final MetricsCompressor compressor;
        private final ILogger logger;
        private final UnaryOperator<MetricDescriptor> addPrefixFn;

        JobMetricsCollector(long executionId, @Nonnull Member member, @Nonnull ILogger logger) {
            Objects.requireNonNull(member, "member");
            this.logger = Objects.requireNonNull(logger, "logger");

            this.executionId = executionId;
            this.addPrefixFn = JobMetricsUtil.addMemberPrefixFn(member);
            this.compressor = new MetricsCompressor();
        }

        @Override
        public void collectLong(MetricDescriptor descriptor, long value) {
            System.out.println("bbb: " + descriptor + ", v=" + value);
            Long executionId = JobMetricsUtil.getExecutionIdFromMetricsDescriptor(descriptor);
            if (this.executionId.equals(executionId)) {
                System.out.println("taken");
                compressor.addLong(addPrefixFn.apply(descriptor), value);
            }
        }

        @Override
        public void collectDouble(MetricDescriptor descriptor, double value) {
            Long executionId = JobMetricsUtil.getExecutionIdFromMetricsDescriptor(descriptor);
            if (this.executionId.equals(executionId)) {
                compressor.addDouble(addPrefixFn.apply(descriptor), value);
            }
        }

        @Override
        public void collectException(MetricDescriptor descriptor, Exception e) {
            Long executionId = JobMetricsUtil.getExecutionIdFromMetricsDescriptor(descriptor);
            if (this.executionId.equals(executionId)) {
                logger.warning("Exception when rendering job metrics: " + e, e);
            }
        }

        @Override
        public void collectNoValue(MetricDescriptor descriptor) { }

        @Nonnull
        public RawJobMetrics getMetrics() {
            return RawJobMetrics.of(compressor.getBlobAndReset());
        }
    }
}
