/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.impl.deployment.JetClassLoader;
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
import java.security.AccessController;
import java.security.PrivilegedAction;
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
     * the coordinator. Therefore the checker cannot confirm with the
     * coordinator if it still exists. We terminate these jobs after a timeout.
     * However, the timeout has to be long enough because if the job happens to
     * be initialized later, we'll lose data and we won't eve detect it. It can
     * also happen that we lose a DONE_ITEM and the job will get stuck, though
     * that's better than incorrect results.
     */
    private static final long UNINITIALIZED_CONTEXT_MAX_AGE_NS = MINUTES.toNanos(5);

    private final Object mutex = new Object();

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final TaskletExecutionService taskletExecutionService;
    private final JobRepository jobRepository;

    private final Set<Long> executionContextJobIds = newSetFromMap(new ConcurrentHashMap<>());

    // key: executionId
    private final ConcurrentMap<Long, ExecutionContext> executionContexts = new ConcurrentHashMap<>();

    // The type of classLoaders field is CHM and not ConcurrentMap because we
    // rely on specific semantics of computeIfAbsent. ConcurrentMap.computeIfAbsent
    // does not guarantee at most one computation per key.
    // key: jobId
    private final ConcurrentHashMap<Long, JetClassLoader> classLoaders = new ConcurrentHashMap<>();

    @Probe(name = MetricNames.JOB_EXECUTIONS_STARTED)
    private final Counter executionStarted = MwCounter.newMwCounter();
    @Probe(name = MetricNames.JOB_EXECUTIONS_COMPLETED)
    private final Counter executionCompleted = MwCounter.newMwCounter();

    private final Function<? super Long, ? extends ExecutionContext> newLightJobExecutionContextFunction;

    private final ScheduledFuture<?> lightExecutionsSender;

    JobExecutionService(NodeEngineImpl nodeEngine, TaskletExecutionService taskletExecutionService,
                        JobRepository jobRepository) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.taskletExecutionService = taskletExecutionService;
        this.jobRepository = jobRepository;

        newLightJobExecutionContextFunction = execId -> new ExecutionContext(nodeEngine, execId, execId, true);

        // register metrics
        MetricsRegistry registry = nodeEngine.getMetricsRegistry();
        MetricDescriptor descriptor = registry.newMetricDescriptor()
                .withTag(MetricTags.MODULE, "jet");
        registry.registerStaticMetrics(descriptor, this);

        this.lightExecutionsSender = nodeEngine.getExecutionService().scheduleWithRepetition(
                this::checkLightExecutions, 0, 1, SECONDS);
    }

    public Long getExecutionIdForJobId(long jobId) {
        return executionContexts.values().stream()
                                .filter(ec -> ec.jobId() == jobId)
                                .findAny()
                                .map(ExecutionContext::executionId)
                                .orElse(null);
    }

    public ClassLoader getClassLoader(JobConfig config, long jobId) {
        return classLoaders.computeIfAbsent(jobId,
                k -> AccessController.doPrivileged(
                        (PrivilegedAction<JetClassLoader>) () -> {
                            ClassLoader parent = config.getClassLoaderFactory() != null
                                    ? config.getClassLoaderFactory().getJobClassLoader()
                                    : nodeEngine.getConfigClassLoader();
                            return new JetClassLoader(nodeEngine, parent, config.getName(), jobId, jobRepository);
                        }));
    }

    public ExecutionContext getExecutionContext(long executionId) {
        return executionContexts.get(executionId);
    }

    /**
     * Gets the execution context or creates it, if it doesn't exist. If
     * we're creating it, we assume it's for a light job and that the
     * jobId == executionId.
     */
    public ExecutionContext getOrCreateExecutionContext(long executionId) {
        return executionContexts.computeIfAbsent(executionId, newLightJobExecutionContextFunction);
    }

    Collection<ExecutionContext> getExecutionContexts() {
        return executionContexts.values();
    }

    Map<SenderReceiverKey, SenderTasklet> getSenderMap(long executionId) {
        ExecutionContext ctx = executionContexts.get(executionId);
        return ctx != null ? ctx.senderMap() : null;
    }

    public void shutdown() {
        lightExecutionsSender.cancel(false);
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
    private void cancelAllExecutions(String reason) {
        for (ExecutionContext exeCtx : executionContexts.values()) {
            LoggingUtil.logFine(logger, "Completing %s locally. Reason: %s",
                    exeCtx.jobNameAndExecutionId(), reason);
            exeCtx.terminateExecution(null);
        }
    }

    /**
     * Cancels executions that contain the leaving address as the coordinator or a
     * job participant
     */
    void onMemberRemoved(Address address) {
        executionContexts.values().stream()
             // note that coordinator might not be a participant (in case it is a lite member)
             .filter(exeCtx -> exeCtx.coordinator() != null
                     && (exeCtx.coordinator().equals(address) || exeCtx.hasParticipant(address)))
             .forEach(exeCtx -> {
                 LoggingUtil.logFine(logger, "Completing %s locally. Reason: Member %s left the cluster",
                         exeCtx.jobNameAndExecutionId(), address);
                 exeCtx.terminateExecution(null);
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

        Set<Address> addresses = participants.stream().map(MemberInfo::getAddress).collect(toSet());
        ClassLoader jobCl = getClassLoader(plan.getJobConfig(), jobId);
        doWithClassLoader(jobCl, () -> execCtx.initialize(coordinator, addresses, plan));

        // initial log entry with all of jobId, jobName, executionId
        if (logger.isFineEnabled()) {
            logger.fine("Execution plan for light job ID=" + idToString(jobId)
                    + ", jobName=" + (execCtx.jobName() != null ? '\'' + execCtx.jobName() + '\'' : "null")
                    + ", executionId=" + idToString(executionId) + " initialized, will start the job");
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
        assertIsMaster(jobId, executionId, coordinator);
        verifyClusterInformation(jobId, executionId, coordinator, coordinatorMemberListVersion, participants);
        failIfNotRunning();

        ExecutionContext execCtx;
        synchronized (mutex) {
            addExecutionContextJobId(jobId, executionId, coordinator);
            execCtx = new ExecutionContext(nodeEngine, jobId, executionId, false);
            ExecutionContext oldContext = executionContexts.put(executionId, execCtx);
            if (oldContext != null) {
                throw new RuntimeException("Duplicate ExecutionContext for execution " + Util.idToString(executionId));
            }
        }

        Set<Address> addresses = participants.stream().map(MemberInfo::getAddress).collect(toSet());
        ClassLoader jobCl = getClassLoader(plan.getJobConfig(), jobId);
        doWithClassLoader(jobCl, () -> execCtx.initialize(coordinator, addresses, plan));

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
            throw new TopologyChangedException(String.format(
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
        executionContexts.remove(executionContext.executionId());
        JetClassLoader removedClassLoader = classLoaders.remove(executionContext.jobId());
        try {
            doWithClassLoader(removedClassLoader, () -> executionContext.completeExecution(error));
        } finally {
            executionCompleted.inc();
            removedClassLoader.shutdown();
            executionContextJobIds.remove(executionContext.jobId());
            logger.fine("Completed execution of " + executionContext.jobNameAndExecutionId());
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
        ExecutionContext execCtx = assertExecutionContext(coordinator, jobId, executionId, "ExecuteJobOperation");
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
                        terminalMetrics = RawJobMetrics.empty();
                    }
                    return terminalMetrics;
                })
                .whenComplete(withTryCatch(logger, (i, e) -> {
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

    int numberOfExecutions() {
        return executionContexts.size();
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
     * See javadoc at {@link CheckLightJobsOperation}.
     */
    private void checkLightExecutions() {
        try {
            long uninitializedContextThreshold = System.nanoTime() - UNINITIALIZED_CONTEXT_MAX_AGE_NS;
            Map<Address, List<Long>> executionsPerMember = new HashMap<>();
            for (ExecutionContext ctx : executionContexts.values()) {
                if (!ctx.isLightJob()) {
                    continue;
                }
                Address coordinator = ctx.coordinator();
                if (coordinator != null) {
                    executionsPerMember
                            .computeIfAbsent(coordinator, k -> new ArrayList<>())
                            .add(ctx.executionId());
                } else {
                    if (uninitializedContextThreshold <= ctx.getCreatedOn()) {
                        LoggingUtil.logFine(logger, "Terminating light job %s because it wasn't initialized during %d seconds",
                                idToString(ctx.executionId()), NANOSECONDS.toSeconds(UNINITIALIZED_CONTEXT_MAX_AGE_NS));
                        ctx.terminateExecution(TerminationMode.CANCEL_FORCEFUL);
                    }
                }
            }

            for (Entry<Address, List<Long>> en : executionsPerMember.entrySet()) {
                long[] executionIds = en.getValue().stream().mapToLong(Long::longValue).toArray();
                Operation op = new CheckLightJobsOperation(executionIds);
                InvocationFuture<long[]> future = nodeEngine.getOperationService()
                        .createInvocationBuilder(JetService.SERVICE_NAME, op, en.getKey())
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
                                    + " because the coordinator doesn't know it or has it cancelled");
                            execCtx.terminateExecution(TerminationMode.CANCEL_FORCEFUL);
                        }
                    }
                });
            }
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
            // This can happen if:
            // - InitOp wasn't handled yet
            // - ExecutionContext was created due to a data packet received
            // The TerminateOp is always sent after InitOp, but it can happen to be handled
            // first on the target member - we ignore and rely on the CheckLightJobsOperation to clean it up.
            // It can't happen for normal jobs
            assert executionContext.isLightJob() : "null coordinator for non-light job";
            return;
        }
        if (!coordinator.equals(callerAddress)) {
            throw new IllegalStateException(String.format(
                    "%s, originally from coordinator %s, cannot do 'terminateExecution' by coordinator %s and execution %s",
                    executionContext.jobNameAndExecutionId(), coordinator, callerAddress, idToString(executionId)));
        }
        executionContext.terminateExecution(mode);
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
