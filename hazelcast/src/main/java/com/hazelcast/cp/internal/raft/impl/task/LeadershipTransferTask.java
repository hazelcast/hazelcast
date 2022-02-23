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

package com.hazelcast.cp.internal.raft.impl.task;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.TriggerLeaderElection;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.state.LeaderState;
import com.hazelcast.cp.internal.raft.impl.state.LeadershipTransferState;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.logging.ILogger;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static java.lang.Math.max;

/**
 * Sends a {@link TriggerLeaderElection} request to the endpoint given in
 * {@link LeadershipTransferState#endpoint()}. Before sending this request,
 * the append request backoff state of the endpoint is reset and
 * an {@link AppendRequest} is also sent.
 * <p>
 * This task waits until all appended entries are committed in the current
 * leader before starting the leadership transfer, and reschedules itself if
 * the local Raft node is still leader or there are uncommitted log entries.
 */
public class LeadershipTransferTask implements Runnable {

    private final RaftNodeImpl raftNode;
    private final int retryCount;
    private final int maxRetryCount;

    LeadershipTransferTask(RaftNodeImpl raftNode, int maxRetryCount) {
        this(raftNode, 0, maxRetryCount);
    }

    private LeadershipTransferTask(RaftNodeImpl raftNode, int retryCount, int maxRetryCount) {
        this.raftNode = raftNode;
        this.retryCount = retryCount;
        this.maxRetryCount = maxRetryCount;
    }

    @Override
    public void run() {
        ILogger logger = raftNode.getLogger(getClass());
        RaftState state = raftNode.state();
        LeaderState leaderState = state.leaderState();

        if (leaderState == null) {
            logger.fine("Not retrying leadership transfer since not leader...");
            return;
        }

        LeadershipTransferState leadershipTransferState = state.leadershipTransferState();
        checkTrue(leadershipTransferState != null, "No leadership transfer state!");

        if (retryCount == maxRetryCount) {
            String msg = "Leadership transfer to " + leadershipTransferState.endpoint() + " timed out!";
            logger.warning(msg);
            state.completeLeadershipTransfer(new IllegalStateException(msg));
            return;
        }

        RaftEndpoint targetEndpoint = leadershipTransferState.endpoint();

        if (state.commitIndex() < state.log().lastLogOrSnapshotIndex()) {
            logger.warning("Waiting until all appended entries to be committed before transferring leadership to "
                    + targetEndpoint);
            reschedule();
            return;
        }

        if (retryCount > 0) {
            logger.fine("Retrying leadership transfer to " + leadershipTransferState.endpoint());
        } else {
            logger.info("Transferring leadership to " + leadershipTransferState.endpoint());
        }

        leaderState.getFollowerState(targetEndpoint).appendRequestAckReceived();
        raftNode.sendAppendRequest(targetEndpoint);

        LogEntry entry = state.log().lastLogOrSnapshotEntry();
        raftNode.send(new TriggerLeaderElection(raftNode.getLocalMember(), state.term(), entry.term(), entry.index()),
                targetEndpoint);

        reschedule();
    }

    private void reschedule() {
        long delayMillis = max(1, raftNode.getLeaderElectionTimeoutInMillis() * 2 / maxRetryCount);
        raftNode.schedule(new LeadershipTransferTask(raftNode, retryCount + 1, maxRetryCount), delayMillis);
    }
}
