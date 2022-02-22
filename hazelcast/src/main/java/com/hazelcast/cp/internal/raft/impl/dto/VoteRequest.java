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

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Struct for VoteRequest RPC.
 * <p>
 * See <i>5.2 Leader election</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 * <p>
 * Invoked by candidates to gather votes (§5.2).
 */
public class VoteRequest implements IdentifiedDataSerializable {

    private RaftEndpoint candidate;
    private int term;
    private int lastLogTerm;
    private long lastLogIndex;
    private boolean disruptive;

    public VoteRequest() {
    }

    public VoteRequest(RaftEndpoint candidate, int term, int lastLogTerm, long lastLogIndex, boolean disruptive) {
        this.term = term;
        this.candidate = candidate;
        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
        this.disruptive = disruptive;
    }

    public RaftEndpoint candidate() {
        return candidate;
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

    public boolean isDisruptive() {
        return disruptive;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftDataSerializerHook.VOTE_REQUEST;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(term);
        out.writeObject(candidate);
        out.writeInt(lastLogTerm);
        out.writeLong(lastLogIndex);
        out.writeBoolean(disruptive);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        candidate = in.readObject();
        lastLogTerm = in.readInt();
        lastLogIndex = in.readLong();
        disruptive = in.readBoolean();
    }

    @Override
    public String toString() {
        return "VoteRequest{" + "candidate=" + candidate + ", term=" + term + ", lastLogTerm=" + lastLogTerm
                + ", lastLogIndex=" + lastLogIndex + ", disruptive=" + disruptive + '}';
    }

}
