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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Struct for successful response to AppendEntries RPC.
 * <p>
 * See <i>5.3 Log replication</i> section of
 * <i>In Search of an Understandable Consensus Algorithm</i>
 * paper by <i>Diego Ongaro</i> and <i>John Ousterhout</i>.
 *
 * @see AppendRequest
 * @see AppendFailureResponse
 */
public class AppendSuccessResponse implements IdentifiedDataSerializable {

    private Endpoint follower;
    private int term;
    private long lastLogIndex;
    private long queryRound;

    public AppendSuccessResponse() {
    }

    public AppendSuccessResponse(Endpoint follower, int term, long lastLogIndex, long queryRound) {
        this.follower = follower;
        this.term = term;
        this.lastLogIndex = lastLogIndex;
        this.queryRound = queryRound;
    }

    public Endpoint follower() {
        return follower;
    }

    public int term() {
        return term;
    }

    public long lastLogIndex() {
        return lastLogIndex;
    }

    public long queryRound() {
        return queryRound;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftDataSerializerHook.APPEND_SUCCESS_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(term);
        out.writeObject(follower);
        out.writeLong(lastLogIndex);
        out.writeLong(queryRound);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        follower = in.readObject();
        lastLogIndex = in.readLong();
        queryRound = in.readLong();
    }

    @Override
    public String toString() {
        return "AppendSuccessResponse{" + "follower=" + follower + ", term=" + term  + ", lastLogIndex="
                + lastLogIndex + ", queryRound=" + queryRound + '}';
    }

}
