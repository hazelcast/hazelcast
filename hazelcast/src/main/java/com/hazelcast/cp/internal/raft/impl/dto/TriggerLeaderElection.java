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

package com.hazelcast.cp.internal.raft.impl.dto;

import com.hazelcast.cp.internal.raft.impl.RaftDataSerializerHook;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Struct for the leadership transfer logic.
 * <p>
 * See <i>4.2.3 Disruptive servers</i> section
 * of the Raft dissertation.
 */
public class TriggerLeaderElection implements IdentifiedDataSerializable {

    private RaftEndpoint leader;
    private int term;
    private int lastLogTerm;
    private long lastLogIndex;

    public TriggerLeaderElection() {
    }

    public TriggerLeaderElection(RaftEndpoint leader, int term, int lastLogTerm, long lastLogIndex) {
        this.leader = leader;
        this.term = term;
        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
    }

    public RaftEndpoint leader() {
        return leader;
    }

    public int term() {
        return term;
    }

    public int lastLogTerm() {
        return lastLogTerm;
    }

    public long lastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftDataSerializerHook.TRIGGER_LEADER_ELECTION;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(leader);
        out.writeInt(term);
        out.writeInt(lastLogTerm);
        out.writeLong(lastLogIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        leader = in.readObject();
        term = in.readInt();
        lastLogTerm = in.readInt();
        lastLogIndex = in.readLong();
    }

    @Override
    public String toString() {
        return "TriggerLeaderElection{" + "leader=" + leader + ", term=" + term + ", lastLogTerm=" + lastLogTerm
                + ", lastLogIndex=" + lastLogIndex + '}';
    }

}
