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

package com.hazelcast.cp.internal.datastructures.spi.operation;

import com.hazelcast.cp.internal.datastructures.RaftDataServiceDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.spi.RaftRemoteService;

import java.io.IOException;

/**
 * Destroys the distributed object with the given name
 * on the requested Raft group
 */
public class DestroyRaftObjectOp extends RaftOp implements IdentifiedDataSerializable {

    private String serviceName;
    private String objectName;

    public DestroyRaftObjectOp() {
    }

    public DestroyRaftObjectOp(String serviceName, String objectName) {
        this.serviceName = serviceName;
        this.objectName = objectName;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        RaftRemoteService service = getService();
        return service.destroyRaftObject(groupId, objectName);
    }

    @Override
    protected String getServiceName() {
        return serviceName;
    }

    @Override
    public int getFactoryId() {
        return RaftDataServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftDataServiceDataSerializerHook.DESTROY_RAFT_OBJECT_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(serviceName);
        out.writeString(objectName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        serviceName = in.readString();
        objectName = in.readString();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", serviceName=").append(serviceName)
          .append(", objectName=").append(objectName);
    }
}
