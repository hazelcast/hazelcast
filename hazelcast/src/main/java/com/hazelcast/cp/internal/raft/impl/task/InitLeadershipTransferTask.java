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
import com.hazelcast.cp.internal.raft.impl.RaftNodeStatus;
import com.hazelcast.cp.internal.raft.impl.state.LeaderState;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.spi.impl.InternalCompletableFuture;

/**
 * Initializes the leadership transfer process, if the local Raft node
 * is the leader with {@link RaftNodeStatus#ACTIVE} status and the given
 * endpoint is a committed group member.
 */
public class InitLeadershipTransferTask implements Runnable {

    private static final int LEADERSHIP_TRANSFER_RETRY_COUNT = 5;

    private final RaftNodeImpl raftNode;
    private final RaftEndpoint targetEndpoint;
    private final InternalCompletableFuture resultFuture;

    public InitLeadershipTransferTask(RaftNodeImpl raftNode, RaftEndpoint targetEndpoint,
                                      InternalCompletableFuture resultFuture) {
        this.raftNode = raftNode;
        this.targetEndpoint = targetEndpoint;
        this.resultFuture = resultFuture;
    }

    @Override
    public void run() {
        if (!raftNode.getCommittedMembers().contains(targetEndpoint)) {
            resultFuture.completeExceptionally(new IllegalArgumentException("Cannot transfer leadership to " + targetEndpoint
                    + " because it is not in the committed group member list!"));
            return;
        }

        if (raftNode.getStatus() != RaftNodeStatus.ACTIVE) {
            resultFuture.completeExceptionally(new IllegalStateException("Cannot transfer leadership to " + targetEndpoint
                    + " because the status is " + raftNode.getStatus()));
            return;
        }


        RaftState state = raftNode.state();
        LeaderState leaderState = state.leaderState();

        if (leaderState == null) {
            resultFuture.completeExceptionally(new IllegalStateException("Cannot transfer leadership to " + targetEndpoint
                    + " because I am not the leader!"));
            return;
        }

        if (raftNode.getLocalMember().equals(targetEndpoint)) {
            raftNode.getLogger(getClass()).warning("I am already the leader... There is no leadership transfer to myself.");
            resultFuture.complete(null);
            return;
        }

        if (state.initLeadershipTransfer(targetEndpoint, resultFuture)) {
            new LeadershipTransferTask(raftNode, LEADERSHIP_TRANSFER_RETRY_COUNT).run();
        }
    }
}
