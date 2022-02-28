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

import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;

import static com.hazelcast.cp.internal.raft.impl.RaftRole.CANDIDATE;

/**
 * LeaderElectionTimeoutTask is scheduled by {@link LeaderElectionTask}
 * to trigger leader election again if one is not elected after
 * leader election timeout.
 */
public class LeaderElectionTimeoutTask extends RaftNodeStatusAwareTask implements Runnable {

    LeaderElectionTimeoutTask(RaftNodeImpl raftNode) {
        super(raftNode);
    }

    @Override
    protected void innerRun() {
        if (raftNode.state().role() != CANDIDATE) {
            return;
        }
        logger.warning("Leader election for term: " + raftNode.state().term() + " has timed out!");
        new LeaderElectionTask(raftNode, false).run();
    }
}
