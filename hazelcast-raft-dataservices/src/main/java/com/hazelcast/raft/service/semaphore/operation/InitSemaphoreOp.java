/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.semaphore.operation;

import com.hazelcast.core.ISemaphore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.IndeterminateOperationStateAware;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreDataSerializerHook;
import com.hazelcast.raft.service.semaphore.RaftSemaphoreService;

import java.io.IOException;

/**
 * Operation for {@link ISemaphore#init(int)}
 *
 * @see com.hazelcast.raft.service.semaphore.RaftSemaphore#init(int)
 */
public class InitSemaphoreOp extends RaftOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private String name;
    private int permits;

    public InitSemaphoreOp() {
    }

    public InitSemaphoreOp(String name, int permits) {
        this.name = name;
        this.permits = permits;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftSemaphoreService service = getService();
        return service.initSemaphore(groupId, name, permits);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return false;
    }

    @Override
    protected String getServiceName() {
        return RaftSemaphoreService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftSemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftSemaphoreDataSerializerHook.INIT_SEMAPHORE_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(permits);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        permits = in.readInt();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", name=").append(name)
          .append(", permits=").append(permits);
    }
}
