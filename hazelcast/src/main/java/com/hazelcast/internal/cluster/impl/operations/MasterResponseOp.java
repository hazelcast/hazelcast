/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.cluster.Address;

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;
import java.util.UUID;

/** Operation sent by any node to set the master on the receiver */
public class MasterResponseOp extends AbstractClusterOperation implements Versioned {

    protected Address masterAddress;
    protected UUID masterUuid;

    public MasterResponseOp() {
    }

    public MasterResponseOp(Address originAddress, UUID originUuid) {
        this.masterAddress = originAddress;
        this.masterUuid = originUuid;
    }

    @Override
    public void run() {
        ClusterServiceImpl clusterService = getService();
        clusterService.getClusterJoinManager().handleMasterResponse(
                masterAddress,
                masterUuid,
                getCallerAddress(),
                getCallerUuid()
        );
    }

    public Address getMasterAddress() {
        return masterAddress;
    }

    public UUID getMasterUuid() {
        return masterUuid;
    }

    @Override
    protected void readInternal(final ObjectDataInput in) throws IOException {
        masterAddress = in.readObject();
        if (in.getVersion().isGreaterOrEqual(Versions.V5_1)) {
            masterUuid = UUIDSerializationUtil.readUUID(in);
        }
    }

    @Override
    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        out.writeObject(masterAddress);
        if (out.getVersion().isGreaterOrEqual(Versions.V5_1)) {
            UUIDSerializationUtil.writeUUID(out, masterUuid);
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", master=").append(masterAddress);
        if (masterUuid != null) {
            sb.append("-").append(masterUuid);
        }
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.MASTER_RESPONSE;
    }
}
