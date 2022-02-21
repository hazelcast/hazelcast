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
 * Struct for PreVoteRequest RPC.
 * <p>
 * See <i>Four modifications for the Raft consensus algorithm</i>
 * by Henrik Ingo.
 *
 * @see VoteRequest
 */
public class PreVoteRequest implements IdentifiedDataSerializable {

    private RaftEndpoint candidate;
    private int nextTerm;
    private int lastLogTerm;
    private long lastLogIndex;

    public PreVoteRequest() {
    }

    public PreVoteRequest(RaftEndpoint candidate, int nextTerm, int lastLogTerm, long lastLogIndex) {
        this.nextTerm = nextTerm;
        this.candidate = candidate;
        this.lastLogTerm = lastLogTerm;
        this.lastLogIndex = lastLogIndex;
    }

    public RaftEndpoint candidate() {
        return candidate;
    }

    public int nextTerm() {
        return nextTerm;
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
        return RaftDataSerializerHook.PRE_VOTE_REQUEST;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(nextTerm);
        out.writeObject(candidate);
        out.writeInt(lastLogTerm);
        out.writeLong(lastLogIndex);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        nextTerm = in.readInt();
        candidate = in.readObject();
        lastLogTerm = in.readInt();
        lastLogIndex = in.readLong();
    }

    @Override
    public String toString() {
        return "PreVoteRequest{" + "candidate=" + candidate + ", nextTerm=" + nextTerm + ", lastLogTerm=" + lastLogTerm
                + ", lastLogIndex=" + lastLogIndex + '}';
    }

}
