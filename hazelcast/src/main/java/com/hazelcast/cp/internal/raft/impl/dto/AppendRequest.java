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

package com.hazelcast.cp.internal.raft.impl.dto;

import com.hazelcast.core.Endpoint;
import com.hazelcast.cp.internal.raft.impl.RaftDataSerializerHook;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;

/**
 * Struct for AppendEntries RPC.
 * <p>
 * See <i>5.3 Log replication</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 * <p>
 * Invoked by leader to replicate log entries (§5.3);
 * also used as heartbeat (§5.2).
 */
public class AppendRequest implements IdentifiedDataSerializable {

    private Endpoint leader;
    private int term;
    private int prevLogTerm;
    private long prevLogIndex;
    private long leaderCommitIndex;
    private LogEntry[] entries;

    public AppendRequest() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public AppendRequest(Endpoint leader, int term, int prevLogTerm, long prevLogIndex, long leaderCommitIndex,
            LogEntry[] entries) {
        this.term = term;
        this.leader = leader;
        this.prevLogTerm = prevLogTerm;
        this.prevLogIndex = prevLogIndex;
        this.leaderCommitIndex = leaderCommitIndex;
        this.entries = entries;
    }

    public Endpoint leader() {
        return leader;
    }

    public int term() {
        return term;
    }

    public int prevLogTerm() {
        return prevLogTerm;
    }

    public long prevLogIndex() {
        return prevLogIndex;
    }

    public long leaderCommitIndex() {
        return leaderCommitIndex;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public LogEntry[] entries() {
        return entries;
    }

    public int entryCount() {
        return entries.length;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.APPEND_REQUEST;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(term);
        out.writeObject(leader);
        out.writeInt(prevLogTerm);
        out.writeLong(prevLogIndex);
        out.writeLong(leaderCommitIndex);

        out.writeInt(entries.length);
        for (LogEntry entry : entries) {
            out.writeObject(entry);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        leader = in.readObject();
        prevLogTerm = in.readInt();
        prevLogIndex = in.readLong();
        leaderCommitIndex = in.readLong();

        int len = in.readInt();
        entries = new LogEntry[len];
        for (int i = 0; i < len; i++) {
            entries[i] = in.readObject();
        }
    }

    @Override
    public String toString() {
        return "AppendRequest{" + "leader=" + leader + ", term=" + term + ", prevLogTerm=" + prevLogTerm
                + ", prevLogIndex=" + prevLogIndex + ", leaderCommitIndex=" + leaderCommitIndex + ", entries=" + Arrays
                .toString(entries) + '}';
    }

}
