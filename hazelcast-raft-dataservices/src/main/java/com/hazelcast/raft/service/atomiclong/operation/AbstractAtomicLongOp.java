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

package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLong;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;

import java.io.IOException;

/**
 * Base class for operations of Raft-based atomic long
 */
public abstract class AbstractAtomicLongOp extends RaftOp implements IdentifiedDataSerializable {

    private String name;

    AbstractAtomicLongOp() {
    }

    AbstractAtomicLongOp(String name) {
        this.name = name;
    }

    RaftAtomicLong getAtomicLong(RaftGroupId groupId) {
        RaftAtomicLongService service = getService();
        return service.getAtomicLong(groupId, name);
    }

    @Override
    public final String getServiceName() {
        return RaftAtomicLongService.SERVICE_NAME;
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
    public final int getFactoryId() {
        return RaftAtomicLongDataSerializerHook.F_ID;
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", name=").append(name);
    }
}
