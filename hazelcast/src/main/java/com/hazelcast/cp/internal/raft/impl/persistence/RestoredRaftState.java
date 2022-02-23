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

package com.hazelcast.cp.internal.raft.impl.persistence;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNode;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

/**
 * Contains persisted and restored state of a {@link RaftNode}.
 */
public class RestoredRaftState {

    private final RaftEndpoint localEndpoint;
    private final Collection<RaftEndpoint> initialMembers;
    private final int term;
    private final RaftEndpoint votedFor;
    private final SnapshotEntry snapshot;
    private final LogEntry[] entries;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public RestoredRaftState(
            @Nonnull RaftEndpoint localEndpoint,
            @Nonnull Collection<RaftEndpoint> initialMembers,
            int term,
            @Nullable RaftEndpoint votedFor,
            @Nullable SnapshotEntry snapshot,
            @Nonnull LogEntry[] entries
    ) {
        this.localEndpoint = localEndpoint;
        this.initialMembers = initialMembers;
        this.term = term;
        this.votedFor = votedFor;
        this.snapshot = snapshot;
        this.entries = entries;
    }

    @Nonnull
    public RaftEndpoint localEndpoint() {
        return localEndpoint;
    }

    @Nonnull
    public Collection<RaftEndpoint> initialMembers() {
        return initialMembers;
    }

    public int term() {
        return term;
    }

    @Nullable
    public RaftEndpoint votedFor() {
        return votedFor;
    }

    @Nullable
    public SnapshotEntry snapshot() {
        return snapshot;
    }

    @Nonnull
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public LogEntry[] entries() {
        return entries;
    }

}
