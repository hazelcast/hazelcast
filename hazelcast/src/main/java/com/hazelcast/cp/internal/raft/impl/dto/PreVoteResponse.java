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
 * Struct for response to PreVoteRequest RPC.
 * <p>
 * See <i>Four modifications for the Raft consensus algorithm</i>
 * by Henrik Ingo.
 *
 * @see PreVoteRequest
 * @see VoteResponse
 */
public class PreVoteResponse implements IdentifiedDataSerializable {

    private Endpoint voter;
    private int term;
    private boolean granted;

    public PreVoteResponse() {
    }

    public PreVoteResponse(Endpoint voter, int term, boolean granted) {
        this.voter = voter;
        this.term = term;
        this.granted = granted;
    }

    public Endpoint voter() {
        return voter;
    }

    public int term() {
        return term;
    }

    public boolean granted() {
        return granted;
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftDataSerializerHook.PRE_VOTE_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(term);
        out.writeBoolean(granted);
        out.writeObject(voter);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        granted = in.readBoolean();
        voter = in.readObject();
    }

    @Override
    public String toString() {
        return "PreVoteResponse{" + "voter=" + voter + ", term=" + term + ", granted=" + granted + '}';
    }

}
