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
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import com.hazelcast.jet.impl.deployment.JetClassLoader;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.SenderTasklet;
import com.hazelcast.jet.impl.execution.TaskletExecutionService;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.metrics.RawJobMetrics;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.doWithClassLoader;
import static com.hazelcast.jet.impl.util.Util.jobIdAndExecutionId;
import static java.util.Collections.newSetFromMap;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

/**
 * Service to handle ExecutionContexts on all cluster members. Job-control
 * operations from coordinator are handled here.
 */
public class JobExecutionService implements DynamicMetricsProvider {

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

    JobExecutionService(NodeEngineImpl nodeEngine, TaskletExecutionService taskletExecutionService,
                        JobRepository jobRepository) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.taskletExecutionService = taskletExecutionService;
        this.jobRepository = jobRepository;

        // register metrics
        MetricsRegistry registry = nodeEngine.getMetricsRegistry();
        MetricDescriptor descriptor = registry.newMetricDescriptor()
                .withTag(MetricTags.MODULE, "jet");
        registry.registerStaticMetrics(descriptor, this);
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

    Map<Long, ExecutionContext> getExecutionContextsFor(Address member) {
        return executionContexts.entrySet().stream()
                         .filter(entry -> entry.getValue().hasParticipant(member))
                         .collect(toMap(Entry::getKey, Entry::getValue));
    }

    Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> getSenderMap(long executionId) {
        ExecutionContext ctx = executionContexts.get(executionId);
        return ctx != null ? ctx.senderMap() : null;
    }

    public void shutdown() {
        synchronized (mutex) {
            cancelAllExecutions("Node is shutting down", HazelcastInstanceNotActiveException::new);
        }
    }

    public void reset() {
        cancelAllExecutions("reset", TopologyChangedException::new);
    }

    /**
     * Cancels all ongoing executions using the given failure supplier.
     */
    private void cancelAllExecutions(String reason, Supplier<RuntimeException> exceptionSupplier) {
        executionContexts.values().forEach(exeCtx -> {
            String message = String.format("Completing %s locally. Reason: %s",
                    exeCtx.jobNameAndExecutionId(),
                    reason);
            cancelAndComplete(exeCtx, message, exceptionSupplier.get());
        });
    }

    /**
     * Cancels executions that contain the leaving address as the coordinator or a
     * job participant
     */
    void onMemberRemoved(Address address) {
        executionContexts.values().stream()
             // note that coordinator might not be a participant (in case it is a lite member)
             .filter(exeCtx -> exeCtx.coordinator().equals(address) || exeCtx.hasParticipant(address))
             .forEach(exeCtx -> {
                 String message = String.format("Completing %s locally. Reason: Member %s left the cluster",
                         exeCtx.jobNameAndExecutionId(),
                         address);
                 cancelAndComplete(exeCtx, message, new TopologyChangedException("Topology has been changed."));
             });
    }

    /**
     * Cancel job execution and complete execution without waiting for coordinator
     * to send CompleteOperation.
     */
    private void cancelAndComplete(ExecutionContext exeCtx, String message, Throwable t) {
        try {
            exeCtx.terminateExecution(null).whenComplete(withTryCatch(logger, (r, e) -> {
                long executionId = exeCtx.executionId();
                logger.fine(message);
                completeExecution(executionId, t);
            }));
        } catch (Throwable e) {
            logger.severe(String.format("Local cancellation of %s failed", exeCtx.jobNameAndExecutionId()), e);
        }
    }

    public CompletableFuture<Void> runLightJob(
            long jobId,
            long executionId,
            Address coordinator,
            int coordinatorMemberListVersion,
            Set<MemberInfo> participants,
            ExecutionPlan plan
    ) {
        assert executionId == jobId : "executionId(" + idToString(executionId) + ") != jobId(" + idToString(jobId) + ")";
        Timers.i().jobExecService_runLightJob_verifyClusterInfo.start();
        verifyClusterInformation(jobId, executionId, coordinator, coordinatorMemberListVersion, participants);
        Timers.i().jobExecService_runLightJob_verifyClusterInfo.stop();
        failIfNotRunning();

        Timers.i().jobExecService_runLightJob_synchronization_outer.start();
        ExecutionContext execCtx;
        synchronized (mutex) {
            Timers.i().jobExecService_runLightJob_synchronization_inner.start();
            addExecutionContextJobId(jobId, executionId, coordinator);
            execCtx = executionContexts.computeIfAbsent(executionId,
                    x -> new ExecutionContext(nodeEngine, jobId, executionId, true));
            Timers.i().jobExecService_runLightJob_synchronization_inner.stop();
        }
        Timers.i().jobExecService_runLightJob_synchronization_outer.stop();

        Set<Address> addresses = participants.stream().map(MemberInfo::getAddress).collect(toSet());
        ClassLoader jobCl = getClassLoader(plan.getJobConfig(), jobId);
        doWithClassLoader(jobCl, () -> execCtx.initialize(coordinator, addresses, plan));

        // initial log entry with all of jobId, jobName, executionId
        if (logger.isFineEnabled()) {
            logger.fine("Execution plan for light job ID=" + idToString(jobId)
                    + ", jobName=" + (execCtx.jobName() != null ? '\'' + execCtx.jobName() + '\'' : "null")
                    + ", executionId=" + idToString(executionId) + " initialized, will start the job");
        }

        return beginExecution0(execCtx);
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
    public void completeExecution(long executionId, Throwable error) {
        Timers.i().jobExecService_completeExecution.start();
        ExecutionContext executionContext = executionContexts.remove(executionId);
        if (executionContext != null) {
            JetClassLoader removedClassLoader = classLoaders.remove(executionContext.jobId());
            try {
                doWithClassLoader(removedClassLoader, () ->
                    executionContext.completeExecution(error));
            } finally {
                executionCompleted.inc();
                removedClassLoader.shutdown();
                executionContextJobIds.remove(executionContext.jobId());
                logger.fine("Completed execution of " + executionContext.jobNameAndExecutionId());
            }
        } else {
            logger.fine("Execution " + idToString(executionId) + " not found for completion");
        }
        Timers.i().jobExecService_completeExecution.stop();
    }

    public void updateMetrics(@Nonnull Long executionId, RawJobMetrics metrics) {
        ExecutionContext executionContext = executionContexts.get(executionId);
        if (executionContext != null) {
            executionContext.setJobMetrics(metrics);
        }
    }

    public CompletableFuture<Void> beginExecution(Address coordinator, long jobId, long executionId) {
        ExecutionContext execCtx = assertExecutionContext(coordinator, jobId, executionId, "ExecuteJobOperation");
        logger.info("Start execution of " + execCtx.jobNameAndExecutionId() + " from coordinator " + coordinator);
        return beginExecution0(execCtx);
    }

    public CompletableFuture<Void> beginExecution0(ExecutionContext execCtx) {
        executionStarted.inc();
        return execCtx.beginExecution(taskletExecutionService)
              .whenComplete(withTryCatch(logger, (i, e) -> {
                  completeExecution(execCtx.executionId(), e);
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
}
