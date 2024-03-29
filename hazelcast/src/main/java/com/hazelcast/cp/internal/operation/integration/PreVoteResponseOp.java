/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.operation.integration;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Carries a {@link PreVoteResponse} from a Raft follower to a candidate
 */
public class PreVoteResponseOp extends AsyncRaftOp {

    private PreVoteResponse voteResponse;

    public PreVoteResponseOp() {
    }

    public PreVoteResponseOp(CPGroupId groupId, PreVoteResponse voteResponse) {
        super(groupId);
        this.voteResponse = voteResponse;
    }

    @Override
    public void run() {
        RaftService service = getService();
        service.handlePreVoteResponse(groupId, voteResponse, target);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(voteResponse);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        voteResponse = in.readObject();
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.PRE_VOTE_RESPONSE_OP;
    }
}
