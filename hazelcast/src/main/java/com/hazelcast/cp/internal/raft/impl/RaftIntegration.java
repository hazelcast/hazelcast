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

package com.hazelcast.cp.internal.raft.impl;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshot;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.cp.internal.raft.impl.dto.TriggerLeaderElection;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.VoteResponse;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.concurrent.TimeUnit;

/**
 * Integration abstraction between Raft state machine and the underlying
 * platform which is responsible for task/operation execution &amp; scheduling,
 * message transportation and failure detection.
 */
public interface RaftIntegration {

    /**
     * Returns an {@code ILogger} instance for given name.
     *
     * @param name logger name
     * @return logger instance
     */
    ILogger getLogger(String name);

    /**
     * Returns true if underlying platform is ready to operate,
     * false otherwise.
     *
     * @return true if ready, false otherwise
     */
    boolean isReady();

    /**
     * Returns true if the endpoint is reachable by the time this method
     * is called, false otherwise.
     *
     * @param endpoint endpoint
     * @return true if endpoint is reachable, false otherwise
     */
    boolean isReachable(RaftEndpoint endpoint);

    /**
     * Sends the given {@link PreVoteRequest} DTO to target endpoint
     * to be handled by via {@link RaftNode#handlePreVoteRequest(PreVoteRequest)}.
     *
     * @return true if request is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(PreVoteRequest request, RaftEndpoint target);

    /**
     * Sends the given {@link PreVoteResponse} DTO to target endpoint
     * to be handled via {@link RaftNode#handlePreVoteResponse(PreVoteResponse)}.
     *
     * @return true if response is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(PreVoteResponse response, RaftEndpoint target);

    /**
     * Sends the given {@link VoteRequest} DTO to target endpoint
     * to be handled via {@link RaftNode#handleVoteRequest(VoteRequest)}.
     *
     * @return true if request is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(VoteRequest request, RaftEndpoint target);

    /**
     * Sends the given {@link VoteResponse} DTO to target endpoint
     * to be handled via {@link RaftNode#handleVoteResponse(VoteResponse)}.
     *
     * @return true if response is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(VoteResponse response, RaftEndpoint target);

    /**
     * Sends the given {@link AppendRequest} DTO to target endpoint
     * to be handled via {@link RaftNode#handleAppendRequest(AppendRequest)}.
     *
     * @return true if request is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(AppendRequest request, RaftEndpoint target);

    /**
     * Sends the given {@link AppendSuccessResponse} DTO to target endpoint
     * to be handled via {@link RaftNode#handleAppendResponse(AppendSuccessResponse)}.
     *
     * @return true if response is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(AppendSuccessResponse response, RaftEndpoint target);

    /**
     * Sends the given {@link AppendFailureResponse} DTO to target endpoint
     * to be handled via {@link RaftNode#handleAppendResponse(AppendFailureResponse)}.
     *
     * @return true if response is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(AppendFailureResponse response, RaftEndpoint target);

    /**
     * Sends the given {@link InstallSnapshot} DTO to target endpoint
     * to be handled via {@link RaftNode#handleInstallSnapshot(InstallSnapshot)}.
     *
     * @return true if request is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(InstallSnapshot request, RaftEndpoint target);

    /**
     * Sends the given {@link TriggerLeaderElection} DTO to target endpoint
     * to be handled via {@link RaftNode#handleTriggerLeaderElection(TriggerLeaderElection)}.
     *
     * @return true if request is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(TriggerLeaderElection request, RaftEndpoint target);

    /**
     * Executes the operation on underlying operation execution mechanism
     * and returns its return value.
     *
     * @param operation   raft operation
     * @param commitIndex commit index
     * @return operation execution result
     */
    Object runOperation(Object operation, long commitIndex);

    /**
     * Take a snapshot for the given commit index which is the current commit
     * index
     *
     * @param commitIndex commit index
     * @return snapshot operation to put into the {@link SnapshotEntry}
     */
    Object takeSnapshot(long commitIndex);

    /**
     * Restores the snapshot with the given operation for the given commit
     * index
     *
     * @param operation snapshot operation provided by {@link #takeSnapshot(long)}
     * @param commitIndex commit index of the snapshot
     */
    void restoreSnapshot(Object operation, long commitIndex);

    /**
     * Executes the given task on the underlying task execution mechanism.
     * <p>
     * Please note that all tasks of a single Raft node must be executed
     * in a single-threaded manner and the happens-before relationship
     * must be maintained between given tasks of the Raft node.
     * <p>
     * The underlying platform is free to execute the given task immediately
     * if it fits to the defined guarantees.
     *
     * @param task the task to be executed.
     */
    void execute(Runnable task);

    /**
     * Submits the given task for execution.
     * <p>
     * If the caller is already on the thread that runs the Raft node,
     * the given task cannot be executed immediately and it must be put into
     * the internal task queue for execution in future.
     *
     * @param task to be executed later.
     */
    void submit(Runnable task);

    /**
     * Schedules the task on the underlying platform to be executed after
     * the given delay.
     * <p>
     * Please note that even though the scheduling can be offloaded to another
     * thread, the given task must be executed in a single-threaded manner and
     * the happens-before relationship must be maintained between given tasks
     * of the Raft node.
     *
     * @param task     the task to be executed in future
     * @param delay    the time from now to delay execution
     * @param timeUnit the time unit of the delay
     */
    void schedule(Runnable task, long delay, TimeUnit timeUnit);

    /**
     * Creates a new instance of {@link InternalCompletableFuture}.
     * @return a new future
     */
    InternalCompletableFuture newCompletableFuture();

    /**
     * Returns the entry to be appended if the no-op entry append on leader
     * election feature is enabled.
     * <p>
     * See <a href="https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J">
     * <i>Bug in single-server membership changes</i></a> post by Diego Ongaro for more info.
     */
    Object getAppendedEntryOnLeaderElection();

    /**
     * Returns true if the linearizable read optimization is enabled.
     * <p>
     * See Section 6.4 of the Raft Dissertation for more information about
     * the linearizable read optimization.
     */
    boolean isLinearizableReadOptimizationEnabled();

    /**
     * Returns the CP member instance of the given Raft endpoint
     */
    CPMember getCPMember(RaftEndpoint target);

    /**
     * Called when RaftNode status changes.
     * @param status new status
     */
    void onNodeStatusChange(RaftNodeStatus status);

    /**
     * Called when CP group is destroyed.
     * @param groupId id of CP group
     */
    void onGroupDestroyed(CPGroupId groupId);
}
