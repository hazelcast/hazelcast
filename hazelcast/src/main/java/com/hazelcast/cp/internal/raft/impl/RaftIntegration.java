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

package com.hazelcast.cp.internal.raft.impl;

import com.hazelcast.core.Endpoint;
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshot;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.VoteResponse;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.internal.util.SimpleCompletableFuture;
import com.hazelcast.logging.ILogger;

import java.util.concurrent.TimeUnit;

/**
 * Integration abstraction between Raft state machine and the underlying
 * platform which is responsible for task/operation execution & scheduling,
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
    boolean isReachable(Endpoint endpoint);

    /**
     * Sends the {@link PreVoteRequest} to target endpoint to be handled by
     * its {@link RaftNode#handlePreVoteRequest(PreVoteRequest)}.
     *
     * @return true if request is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(PreVoteRequest request, Endpoint target);

    /**
     * Sends the {@link PreVoteResponse} to target endpoint to be handled by
     * its {@link RaftNode#handlePreVoteResponse(PreVoteResponse)}.
     *
     * @return true if response is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(PreVoteResponse response, Endpoint target);

    /**
     * Sends the {@link VoteRequest} to target endpoint to be handled by
     * its {@link RaftNode#handleVoteRequest(VoteRequest)}.
     *
     * @return true if request is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(VoteRequest request, Endpoint target);

    /**
     * Sends the {@link VoteResponse} to target endpoint to be handled by
     * its {@link RaftNode#handleVoteResponse(VoteResponse)}.
     *
     * @return true if response is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(VoteResponse response, Endpoint target);

    /**
     * Sends the {@link AppendRequest} to target endpoint to be handled by
     * its {@link RaftNode#handleAppendRequest(AppendRequest)}.
     *
     * @return true if request is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(AppendRequest request, Endpoint target);

    /**
     * Sends the {@link AppendSuccessResponse} to target endpoint to be handled
     * by its {@link RaftNode#handleAppendResponse(AppendSuccessResponse)}.
     *
     * @return true if response is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(AppendSuccessResponse response, Endpoint target);

    /**
     * Sends the {@link AppendFailureResponse} to target endpoint to be handled
     * by its {@link RaftNode#handleAppendResponse(AppendFailureResponse)}.
     *
     * @return true if response is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(AppendFailureResponse response, Endpoint target);

    /**
     * Sends the {@link InstallSnapshot} to target endpoint to be handled by
     * its {@link RaftNode#handleInstallSnapshot(InstallSnapshot)}.
     *
     * @return true if request is sent or scheduled to be sent to target,
     *         false otherwise
     */
    boolean send(InstallSnapshot request, Endpoint target);

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
     * Executes the task on underlying task execution mechanism.
     *
     * @param task the task
     */
    void execute(Runnable task);

    /**
     * Schedules the task on underlying scheduling mechanism.
     *
     * @param task  the task
     * @param delay the time from now to delay execution
     * @param timeUnit the time unit of the delay
     */
    void schedule(Runnable task, long delay, TimeUnit timeUnit);

    /**
     * Creates a new instance of {@link SimpleCompletableFuture}.
     * @return a new future
     */
    SimpleCompletableFuture newCompletableFuture();

    /**
     * Returns the entry to be appended if the no-op entry append on leader
     * election feature is enabled.
     * <p>
     * See <a href="https://groups.google.com/forum/#!msg/raft-dev/t4xj6dJTP6E/d2D9LrWRza8J">
     * <i>Bug in single-server membership changes</i></a> post by Diego Ongaro for more info.
     */
    Object getAppendedEntryOnLeaderElection();

    /**
     * Called when RaftNode status changes.
     * @param status new status
     */
    void onNodeStatusChange(RaftNodeStatus status);
}
