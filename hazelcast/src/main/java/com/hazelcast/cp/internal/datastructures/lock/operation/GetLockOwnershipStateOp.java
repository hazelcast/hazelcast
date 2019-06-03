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

package com.hazelcast.cp.internal.datastructures.lock.operation;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.lock.RaftLockService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * @see com.hazelcast.cp.internal.datastructures.lock.RaftLock#lockOwnershipState()
 */
public class GetLockOwnershipStateOp extends RaftOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private String name;

    public GetLockOwnershipStateOp() {
    }

    public GetLockOwnershipStateOp(String name) {
        this.name = name;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        RaftLockService service = getService();
        return service.getLockOwnershipState(groupId, name);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public final String getServiceName() {
        return RaftLockService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftLockDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftLockDataSerializerHook.GET_RAFT_LOCK_OWNERSHIP_STATE_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", name=").append(name);
    }
}
