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

package com.hazelcast.cp.internal.session.operation;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.session.CPSession.CPSessionOwnerType;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.session.RaftSessionService;
import com.hazelcast.cp.internal.session.RaftSessionServiceDataSerializerHook;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Creates a new session for the given endpoint and returns its id.
 * This operation does not check if the given endpoint has another
 * active session on the Raft group.
 */
public class CreateSessionOp extends RaftOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private Address endpoint;

    private String endpointName;

    private CPSessionOwnerType endpointType;

    public CreateSessionOp() {
    }

    public CreateSessionOp(Address endpoint, String endpointName, CPSessionOwnerType endpointType) {
        this.endpoint = endpoint;
        this.endpointName = endpointName;
        this.endpointType = endpointType;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        RaftSessionService service = getService();
        return service.createNewSession(groupId, endpoint, endpointName, endpointType);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public String getServiceName() {
        return RaftSessionService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftSessionServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftSessionServiceDataSerializerHook.CREATE_SESSION_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(endpoint);
        boolean containsEndpointName = (endpointName != null);
        out.writeBoolean(containsEndpointName);
        if (containsEndpointName) {
            out.writeString(endpointName);
        }
        out.writeString(endpointType.name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        endpoint = in.readObject();
        boolean containsEndpointName = in.readBoolean();
        if (containsEndpointName) {
            endpointName = in.readString();
        }
        endpointType = CPSessionOwnerType.valueOf(in.readString());
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", endpoint=").append(endpoint)
          .append(", endpointName=").append(endpointName)
          .append(", endpointType=").append(endpointType);
    }
}
