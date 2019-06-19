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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembershipManager;
import com.hazelcast.internal.cluster.impl.operations.TriggerMemberListPublishOp;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.impl.deployment.JetClassLoader;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.SenderTasklet;
import com.hazelcast.jet.impl.execution.TaskletExecutionService;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;

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
import static com.hazelcast.jet.impl.util.Util.jobIdAndExecutionId;
import static java.util.Collections.newSetFromMap;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

/**
 * Service to handle ExecutionContexts on all cluster members. Job-control
 * operations from coordinator are handled here.
 */
public class JobExecutionService {

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

    JobExecutionService(NodeEngineImpl nodeEngine, TaskletExecutionService taskletExecutionService,
                        JobRepository jobRepository) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.taskletExecutionService = taskletExecutionService;
        this.jobRepository = jobRepository;
    }

    public ClassLoader getClassLoader(JobConfig config, long jobId) {
        return classLoaders.computeIfAbsent(jobId,
                k -> AccessController.doPrivileged(
                        (PrivilegedAction<JetClassLoader>) () -> {
                            ClassLoader parent = config.getClassLoaderFactory() != null
                                    ? config.getClassLoaderFactory().getJobClassLoader()
                                    : nodeEngine.getConfigClassLoader();
                            return new JetClassLoader(
                                    nodeEngine, parent, config.getName(), jobId, jobRepository.getJobResources(jobId)
                            );
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

    public synchronized void shutdown() {
        cancelAllExecutions("Node is shutting down", HazelcastInstanceNotActiveException::new);
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
     * Cancels executions that contain the left address as the coordinator or a
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
    public synchronized void initExecution(
            long jobId, long executionId, Address coordinator, int coordinatorMemberListVersion,
            Set<MemberInfo> participants, ExecutionPlan plan
    ) {
        verifyClusterInformation(jobId, executionId, coordinator, coordinatorMemberListVersion, participants);
        failIfNotRunning();

        if (!executionContextJobIds.add(jobId)) {
            ExecutionContext current = executionContexts.get(executionId);
            if (current != null) {
                throw new IllegalStateException(String.format(
                        "Execution context for %s for coordinator %s already exists for coordinator %s",
                        current.jobNameAndExecutionId(), coordinator, current.coordinator()));
            }

            // search contexts for one with different executionId, but same jobId
            executionContexts.values().stream()
                             .filter(e -> e.jobId() == jobId)
                             .forEach(e -> logger.fine(String.format(
                                     "Execution context for job %s for coordinator %s already exists"
                                             + " with local execution %s for coordinator %s",
                                     idToString(jobId), coordinator, idToString(e.executionId()),
                                     e.coordinator())));

            throw new RetryableHazelcastException();
        }

        Set<Address> addresses = participants.stream().map(MemberInfo::getAddress).collect(toSet());
        ExecutionContext created = new ExecutionContext(nodeEngine, taskletExecutionService,
                jobId, executionId, coordinator, addresses);
        try {
            ClassLoader jobCl = getClassLoader(plan.getJobConfig(), jobId);
            com.hazelcast.jet.impl.util.Util.doWithClassLoader(jobCl, () -> created.initialize(plan));
        } finally {
            ExecutionContext oldContext = executionContexts.put(executionId, created);
            assert oldContext == null : "Duplicate ExecutionContext for execution " + Util.idToString(executionId);
        }

        // initial log entry with all of jobId, jobName, executionId
        logger.info("Execution plan for jobId=" + idToString(jobId)
                + ", jobName=" + (created.jobName() != null ? '\'' + created.jobName() + '\'' : "null")
                + ", executionId=" + idToString(executionId) + " initialized");
    }

    private void verifyClusterInformation(long jobId, long executionId, Address coordinator,
                                          int coordinatorMemberListVersion, Set<MemberInfo> participants) {
        Address masterAddress = nodeEngine.getMasterAddress();
        if (!coordinator.equals(masterAddress)) {
            failIfNotRunning();

            throw new IllegalStateException(String.format(
                    "Coordinator %s cannot initialize %s. Reason: it is not the master, the master is %s",
                    coordinator, jobIdAndExecutionId(jobId, executionId), masterAddress));
        }

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
        ExecutionContext executionContext = executionContexts.remove(executionId);
        if (executionContext != null) {
            JetClassLoader removed = classLoaders.remove(executionContext.jobId());
            try {
                com.hazelcast.jet.impl.util.Util.doWithClassLoader(removed, () ->
                    executionContext.completeExecution(error));
            } finally {
                removed.shutdown();
                executionContextJobIds.remove(executionContext.jobId());
                logger.fine("Completed execution of " + executionContext.jobNameAndExecutionId());
            }
        } else {
            logger.fine("Execution " + idToString(executionId) + " not found for completion");
        }
    }

    public CompletableFuture<Void> beginExecution(Address coordinator, long jobId, long executionId) {
        ExecutionContext execCtx = assertExecutionContext(coordinator, jobId, executionId, "ExecuteJobOperation");
        logger.info("Start execution of " + execCtx.jobNameAndExecutionId() + " from coordinator " + coordinator);
        CompletableFuture<Void> future = execCtx.beginExecution();
        future.whenComplete(withTryCatch(logger, (i, e) -> {
            if (e instanceof CancellationException) {
                logger.fine("Execution of " + execCtx.jobNameAndExecutionId() + " was cancelled");
            } else if (e != null) {
                logger.fine("Execution of " + execCtx.jobNameAndExecutionId()
                        + " completed with failure", e);
            } else {
                logger.fine("Execution of " + execCtx.jobNameAndExecutionId() + " completed");
            }
        }));
        return future;
    }

    int numberOfExecutions() {
        return executionContexts.size();
    }
}
