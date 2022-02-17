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

package com.hazelcast.cp.internal.operation.integration;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.raft.impl.dto.TriggerLeaderElection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Carries a {@link TriggerLeaderElection} from a Raft leader to a follower
 */
public class TriggerLeaderElectionOp extends AsyncRaftOp {

    private TriggerLeaderElection request;

    public TriggerLeaderElectionOp() {
    }

    public TriggerLeaderElectionOp(CPGroupId groupId, TriggerLeaderElection request) {
        super(groupId);
        this.request = request;
    }

    @Override
    public void run() {
        RaftService service = getService();
        service.handleTriggerLeaderElection(groupId, request, target);
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.TRIGGER_LEADER_ELECTION_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(request);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        request = in.readObject();
    }

}
