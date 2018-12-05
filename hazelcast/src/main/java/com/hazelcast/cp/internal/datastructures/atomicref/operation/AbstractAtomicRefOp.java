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

package com.hazelcast.cp.internal.datastructures.atomicref.operation;

import com.hazelcast.cp.internal.datastructures.atomicref.RaftAtomicRef;
import com.hazelcast.cp.internal.datastructures.atomicref.RaftAtomicRefService;
import com.hazelcast.cp.internal.datastructures.atomicref.RaftAtomicReferenceDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;

import java.io.IOException;

/**
 * Base class for operations of Raft-based atomic reference
 */
public abstract class AbstractAtomicRefOp extends RaftOp implements IdentifiedDataSerializable {

    private String name;

    public AbstractAtomicRefOp() {
    }

    AbstractAtomicRefOp(String name) {
        this.name = name;
    }

    RaftAtomicRef getAtomicRef(CPGroupId groupId) {
        RaftAtomicRefService service = getService();
        return service.getAtomicRef(groupId, name);
    }

    @Override
    public final String getServiceName() {
        return RaftAtomicRefService.SERVICE_NAME;
    }

    @Override
    public final int getFactoryId() {
        return RaftAtomicReferenceDataSerializerHook.F_ID;
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
        sb.append(", name=").append(name);
    }
}
