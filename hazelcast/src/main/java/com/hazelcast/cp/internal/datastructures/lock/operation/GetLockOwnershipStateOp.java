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

package com.hazelcast.cp.internal.datastructures.lock.operation;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.lock.Lock;
import com.hazelcast.cp.internal.datastructures.lock.LockDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.lock.LockService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * @see Lock#lockOwnershipState()
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
        LockService service = getService();
        return service.getLockOwnershipState(groupId, name);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public final String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return LockDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return LockDataSerializerHook.GET_RAFT_LOCK_OWNERSHIP_STATE_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", name=").append(name);
    }
}
