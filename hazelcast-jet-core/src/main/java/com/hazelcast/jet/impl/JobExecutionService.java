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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembershipManager;
import com.hazelcast.internal.cluster.impl.operations.TriggerMemberListPublishOp;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.impl.deployment.JetClassLoader;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.SenderTasklet;
import com.hazelcast.jet.impl.execution.TaskletExecutionService;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.operation.ExecuteOperation;
import com.hazelcast.jet.impl.operation.SnapshotOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.idToString;
import static com.hazelcast.jet.impl.util.Util.jobAndExecutionId;
import static java.util.Collections.newSetFromMap;
import static java.util.stream.Collectors.toSet;

public class JobExecutionService {

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final TaskletExecutionService taskletExecutionService;

    private final Set<Long> executionContextJobIds = newSetFromMap(new ConcurrentHashMap<>());

    // key: executionId
    private final ConcurrentMap<Long, ExecutionContext> executionContexts = new ConcurrentHashMap<>();

    // The type of classLoaders field is CHM and not ConcurrentMap because we
    // rely on specific semantics of computeIfAbsent. ConcurrentMap.computeIfAbsent
    // does not guarantee at most one computation per key.
    // key: jobId
    private final ConcurrentHashMap<Long, JetClassLoader> classLoaders = new ConcurrentHashMap<>();

    JobExecutionService(NodeEngineImpl nodeEngine, TaskletExecutionService taskletExecutionService) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.taskletExecutionService = taskletExecutionService;
    }

    public ClassLoader getClassLoader(long jobId, PrivilegedAction<JetClassLoader> action) {
        return classLoaders.computeIfAbsent(jobId, k -> AccessController.doPrivileged(action));
    }

    public ExecutionContext getExecutionContext(long executionId) {
        return executionContexts.get(executionId);
    }

    Map<Long, ExecutionContext> getExecutionContexts() {
        return new HashMap<>(executionContexts);
    }

    Map<Integer, Map<Integer, Map<Address, SenderTasklet>>> getSenderMap(long executionId) {
        ExecutionContext ctx = executionContexts.get(executionId);
        return ctx != null ? ctx.senderMap() : null;
    }

    /**
     * Cancels all ongoing executions using the given failure supplier
     */
    public void reset(String reason, Supplier<RuntimeException> exceptionSupplier) {
        executionContexts.values().forEach(exeCtx -> {
            String message = String.format("Completing %s locally. Reason: %s",
                    jobAndExecutionId(exeCtx.getJobId(), exeCtx.getExecutionId()),
                    reason);
            cancelAndComplete(exeCtx, message, exceptionSupplier.get());
        });
    }

    /**
     * Cancels executions that contain the left address as the coordinator or a
     * job participant
     */
    void onMemberLeave(Address address) {
        executionContexts.values()
                         .stream()
                         .filter(exeCtx -> exeCtx.isCoordinatorOrParticipating(address))
                         .forEach(exeCtx -> {
                             String message = String.format("Completing %s locally. Reason: %s left the cluster",
                                     jobAndExecutionId(exeCtx.getJobId(), exeCtx.getExecutionId()),
                                     address);
                             cancelAndComplete(exeCtx, message, new TopologyChangedException("Topology has been changed"));
                         });
    }

    private void cancelAndComplete(ExecutionContext exeCtx, String message, Throwable t) {
        try {
            exeCtx.cancel().whenComplete(withTryCatch(logger, (r, e) -> {
                long executionId = exeCtx.getExecutionId();
                logger.fine(message);
                completeExecution(executionId, t);
            }));
        } catch (Throwable e) {
            logger.severe(String.format("Local cancellation of %s failed",
                    jobAndExecutionId(exeCtx.getJobId(), exeCtx.getExecutionId())), e);
        }
    }

    /**
     * Initiates the given execution if the local node accepts the coordinator
     * as its master, and has an up-to-date member list information.
     * <ul><li>
     *   If the local node has a stale member list, it retries the init operation
     *   until it receives the new member list from the master.
     * </li><li>
     *     If the local node detects that the member list changed after the init
     *     operation is sent but before executed, then it sends a graceful failure
     *     so that the job init will be retried properly.
     * </li><li>
     *     If there is an already ongoing execution for the given job, then the
     *     init execution is retried.
     * </li></ul>
     */
    void initExecution(
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
                        jobAndExecutionId(jobId, executionId), coordinator, current.getCoordinator()));
            }

            executionContexts.values().stream()
                             .filter(e -> e.getJobId() == jobId)
                             .forEach(e -> logger.fine(String.format(
                                     "Execution context for %s for coordinator %s already exists"
                                             + " with local execution %s for coordinator %s",
                                     jobAndExecutionId(jobId, executionId), coordinator, idToString(e.getJobId()),
                                     e.getCoordinator())));

            throw new RetryableHazelcastException();
        }

        Set<Address> addresses = participants.stream().map(MemberInfo::getAddress).collect(toSet());
        ExecutionContext created = new ExecutionContext(nodeEngine, taskletExecutionService,
                jobId, executionId, coordinator, addresses);
        try {
            created.initialize(plan);
        } finally {
            executionContexts.put(executionId, created);
        }

        logger.info("Execution plan for " + jobAndExecutionId(jobId, executionId) + " initialized");
    }

    private void verifyClusterInformation(long jobId, long executionId, Address coordinator,
                                          int coordinatorMemberListVersion, Set<MemberInfo> participants) {
        Address masterAddress = nodeEngine.getMasterAddress();
        if (!coordinator.equals(masterAddress)) {
            failIfNotRunning();

            throw new IllegalStateException(String.format(
                    "Coordinator %s cannot initialize %s. Reason: it is not the master, the master is %s",
                    coordinator, jobAndExecutionId(jobId, executionId), masterAddress));
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
                    jobAndExecutionId(jobId, executionId), coordinator, localMemberListVersion,
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
                        jobAndExecutionId(jobId, executionId), coordinator, participant,
                        localMemberListVersion, coordinatorMemberListVersion));
            }
        }

        if (!isLocalMemberParticipant) {
            throw new IllegalArgumentException(String.format(
                    "Cannot initialize %s since member %s is not in participants: %s",
                    jobAndExecutionId(jobId, executionId), thisAddress, participants));
        }
    }

    private void failIfNotRunning() {
        if (!nodeEngine.isRunning()) {
            throw new HazelcastInstanceNotActiveException();
        }
    }

    /**
     * Starts execution of the job if the coordinator is verified
     * as the accepted master and the correct initiator.
     */
    public CompletionStage<Void> execute(Address coordinator, long jobId, long executionId,
                                         Consumer<CompletionStage<Void>> doneCallback) {
        ExecutionContext executionContext = verifyAndGetExecutionContext(coordinator, jobId, executionId,
                ExecuteOperation.class.getSimpleName());

        logger.info("Start execution of " + jobAndExecutionId(jobId, executionId) + " from coordinator " + coordinator);

        return executionContext.execute(doneCallback);
    }

    private ExecutionContext verifyAndGetExecutionContext(Address coordinator, long jobId, long executionId,
                                                          String operationName) {
        Address masterAddress = nodeEngine.getMasterAddress();
        if (!coordinator.equals(masterAddress)) {
            failIfNotRunning();

            throw new IllegalStateException(String.format(
                    "Coordinator %s cannot do '%s' for %s: it is not the master, the master is %s",
                    coordinator, operationName, jobAndExecutionId(jobId, executionId), masterAddress));
        }

        failIfNotRunning();

        ExecutionContext executionContext = executionContexts.get(executionId);
        if (executionContext == null) {
            throw new TopologyChangedException(String.format(
                    "%s not found for coordinator %s for '%s'",
                    jobAndExecutionId(jobId, executionId), coordinator, operationName));
        } else if (!executionContext.verify(coordinator, jobId)) {
            throw new IllegalStateException(String.format(
                    "%s, originally from coordinator %s, cannot do '%s' by coordinator %s and execution %s",
                    jobAndExecutionId(jobId, executionContext.getExecutionId()), executionContext.getCoordinator(),
                    operationName, coordinator, idToString(executionId)));
        }

        return executionContext;
    }

    /**
     * Completes and cleans up execution of the given job
     */
    void completeExecution(long executionId, Throwable error) {
        ExecutionContext executionContext = executionContexts.remove(executionId);
        if (executionContext != null) {
            try {
                executionContext.complete(error);
            } finally {
                classLoaders.remove(executionContext.getJobId());
                executionContextJobIds.remove(executionContext.getJobId());
                logger.fine("Completed execution of " + jobAndExecutionId(executionContext.getJobId(), executionId));
            }
        } else {
            logger.fine("Execution " + idToString(executionId) + " not found for completion");
        }
    }

    public CompletionStage<Void> beginSnapshot(Address coordinator, long jobId, long executionId, long snapshotId) {
        ExecutionContext executionContext = verifyAndGetExecutionContext(coordinator, jobId, executionId,
                SnapshotOperation.class.getSimpleName());

        return executionContext.beginSnapshot(snapshotId);
    }
}
