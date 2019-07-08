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

package com.hazelcast.cp.internal.raft.impl.persistence;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;

import java.util.Collection;

/**
 * Contains persisted and restored state of a {@link RaftNode}.
 */
public class RestoredRaftState {

    private RaftEndpoint localEndpoint;
    private Collection<RaftEndpoint> initialMembers;
    private int term;
    private RaftEndpoint votedFor;
    private int lastVoteTerm;
    private SnapshotEntry snapshot;
    private LogEntry[] entries;

    public RestoredRaftState() {
    }

    public RestoredRaftState(RaftEndpoint localEndpoint, Collection<RaftEndpoint> initialMembers, int term, RaftEndpoint votedFor,
                             int lastVoteTerm, SnapshotEntry snapshot, LogEntry[] entries) {
        this.localEndpoint = localEndpoint;
        this.initialMembers = initialMembers;
        this.term = term;
        this.votedFor = votedFor;
        this.lastVoteTerm = lastVoteTerm;
        this.snapshot = snapshot;
        this.entries = entries;
    }

    public RaftEndpoint localEndpoint() {
        return localEndpoint;
    }

    public Collection<RaftEndpoint> initialMembers() {
        return initialMembers;
    }

    public int term() {
        return term;
    }

    public RaftEndpoint votedFor() {
        return votedFor;
    }

    public int lastVoteTerm() {
        return lastVoteTerm;
    }

    public SnapshotEntry snapshot() {
        return snapshot;
    }

    public LogEntry[] entries() {
        return entries;
    }

}
