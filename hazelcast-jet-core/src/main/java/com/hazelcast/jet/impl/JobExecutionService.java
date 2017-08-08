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
import com.hazelcast.jet.TopologyChangedException;
import com.hazelcast.jet.impl.deployment.JetClassLoader;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.ExecutionService;
import com.hazelcast.jet.impl.execution.SenderTasklet;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
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

import static com.hazelcast.jet.impl.util.Util.formatIds;
import static com.hazelcast.jet.impl.util.Util.idToString;
import static java.util.Collections.newSetFromMap;
import static java.util.stream.Collectors.toSet;

public class JobExecutionService {

    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final ExecutionService executionService;

    private final Set<Long> executionContextJobIds = newSetFromMap(new ConcurrentHashMap<>());

    // key: executionId
    private final ConcurrentMap<Long, ExecutionContext> executionContexts = new ConcurrentHashMap<>();

    // The type of classLoaders field is CHM and not ConcurrentMap because we rely on
    // specific semantics of computeIfAbsent. ConcurrentMap.computeIfAbsent does not guarantee
    // at most one computation per key.
    // key: jobId
    private final ConcurrentHashMap<Long, JetClassLoader> classLoaders = new ConcurrentHashMap<>();

    JobExecutionService(NodeEngineImpl nodeEngine, ExecutionService executionService) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.executionService = executionService;
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
            String message = "Completing " + formatIds(exeCtx.getJobId(), exeCtx.getExecutionId())
                    + " locally. Reason: " + reason;
            cancelAndComplete(exeCtx, message, exceptionSupplier.get());
        });
    }

    /**
     * Cancels executions that contain the left address as the coordinator or a job participant
     */
    void onMemberLeave(Address address) {
        executionContexts.values()
                .stream()
                .filter(exeCtx -> exeCtx.isCoordinatorOrParticipating(address))
                .forEach(exeCtx -> {
                    String message = "Completing " + formatIds(exeCtx.getJobId(), exeCtx.getExecutionId())
                            + " locally. Reason: " + address + " left the cluster";
                    cancelAndComplete(exeCtx, message, new TopologyChangedException("Topology has been changed"));
                });
    }

    private void cancelAndComplete(ExecutionContext exeCtx, String message, Throwable t) {
        try {
            exeCtx.cancel().whenComplete((r, e) -> {
                long executionId = exeCtx.getExecutionId();
                logger.fine(message);
                completeExecution(executionId, t);
            });
        } catch (Exception e) {
            logger.severe("Local cancellation of " + formatIds(exeCtx.getJobId(), exeCtx.getExecutionId())
                    + " failed", e);
        }
    }

    /**
     * Initiates the given execution if the local node accepts the coordinator as its master, and has an up-to-date
     * member list information.
     * - If the local node has a stale member list, it retries the init operation until it receives the new member list
     * from the master.
     * - If the local node detects that the member list changed after the init operation is sent but before executed,
     * then it sends a graceful failure so that the job init will be retried properly.
     * - If there is an already ongoing execution for the given job, then the init execution is retried.
     */
    void initExecution(long jobId, long executionId, Address coordinator, int coordinatorMemberListVersion,
                       Set<MemberInfo> participants, ExecutionPlan plan) {
        verifyClusterInformation(jobId, executionId, coordinator, coordinatorMemberListVersion, participants);

        if (!nodeEngine.isRunning()) {
            throw new HazelcastInstanceNotActiveException();
        }

        if (!executionContextJobIds.add(jobId)) {
            ExecutionContext current = executionContexts.get(executionId);
            if (current != null) {
                throw new IllegalStateException("Execution context for " + formatIds(jobId, executionId)
                        + " for coordinator " + coordinator + " already exists for coordinator "
                        + current.getCoordinator());
            }

            executionContexts.values().stream()
                    .filter(e -> e.getJobId() == jobId)
                    .forEach(e -> logger.fine("Execution context for " + formatIds(jobId, executionId)
                            + " for coordinator " + coordinator + " already exists with local execution " +
                            idToString(e.getJobId()) + " for coordinator " + e.getCoordinator()));

            throw new RetryableHazelcastException();
        }

        Set<Address> addresses = participants.stream().map(MemberInfo::getAddress).collect(toSet());
        ExecutionContext created = new ExecutionContext(nodeEngine, executionService,
                jobId, executionId, coordinator, addresses);
        try {
            created.initialize(plan);
        } finally {
            executionContexts.put(executionId, created);
        }

        logger.info("Execution plan for " + formatIds(jobId, executionId) + " initialized");
    }

    private void verifyClusterInformation(long jobId, long executionId, Address coordinator,
                                          int coordinatorMemberListVersion, Set<MemberInfo> participants) {
        Address masterAddress = nodeEngine.getMasterAddress();
        if (!coordinator.equals(masterAddress)) {
            throw new IllegalStateException("Coordinator " + coordinator + " cannot initialize "
                    + formatIds(jobId, executionId) + ". Reason: it is not master, master is " + masterAddress);
        }

        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
        MembershipManager membershipManager = clusterService.getMembershipManager();
        int localMemberListVersion = membershipManager.getMemberListVersion();
        if (coordinatorMemberListVersion > localMemberListVersion) {
            assert masterAddress != nodeEngine.getThisAddress();

            nodeEngine.getOperationService().send(new TriggerMemberListPublishOp(), masterAddress);
            throw new RetryableHazelcastException("Cannot initialize " + formatIds(jobId, executionId)
                    + " for coordinator " + coordinator + ": local member list version: " + localMemberListVersion
                    + ", coordinator member list version: " + coordinatorMemberListVersion);
        }

        for (MemberInfo participant : participants) {
            if (membershipManager.getMember(participant.getAddress(), participant.getUuid()) == null) {
                throw new TopologyChangedException("Cannot initialize " + formatIds(jobId, executionId)
                        + " for coordinator " + coordinator + ": participant: " + participant
                        + " not found in local member list. Local member list version: " + localMemberListVersion
                        + ", coordinator member list version: " + coordinatorMemberListVersion);
            }
        }
    }

    /**
     * Starts execution of the job if the coordinator is verified as the accepted master and the correct initiator.
     */
    public CompletionStage<Void> execute(Address coordinator, long jobId, long executionId,
                                         Consumer<CompletionStage<Void>> doneCallback) {
        Address masterAddress = nodeEngine.getMasterAddress();
        if (!coordinator.equals(masterAddress)) {
            throw new IllegalStateException("Coordinator " + coordinator + " cannot start " + formatIds(jobId, executionId)
                    + ": it is not master, master is: " + masterAddress);
        }

        if (!nodeEngine.isRunning()) {
            throw new HazelcastInstanceNotActiveException();
        }

        ExecutionContext executionContext = executionContexts.get(executionId);
        if (executionContext == null) {
            throw new IllegalStateException(formatIds(jobId, executionId)
                    + " not found for coordinator " + coordinator + " for execution start");
        } else if (!executionContext.verify(coordinator, jobId)) {
            throw new IllegalStateException(formatIds(jobId, executionContext.getExecutionId())
                    + " originally from coordinator " + executionContext.getCoordinator()
                    + " cannot be started by coordinator " + coordinator + " and execution " + idToString(executionId));
        }

        logger.info("Start execution of " + formatIds(jobId, executionId) + " from coordinator " + coordinator);

        return executionContext.execute(doneCallback);
    }

    /**
     * Completes and cleans up execution of the given job
     */
    void completeExecution(long executionId, Throwable error) {
        ExecutionContext executionContext = executionContexts.remove(executionId);
        if (executionContext != null) {
            executionContext.complete(error);
            classLoaders.remove(executionContext.getJobId());
            executionContextJobIds.remove(executionContext.getJobId());
            logger.fine("Completed execution of " + formatIds(executionContext.getJobId(), executionId));
        } else {
            logger.fine("Execution " + idToString(executionId) + " not found for completion");
        }
    }

}
