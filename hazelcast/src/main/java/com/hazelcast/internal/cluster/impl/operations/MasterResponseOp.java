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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/** Operation sent by any node to set the master address on the receiver */
public class MasterResponseOp extends AbstractClusterOperation {

    protected Address masterAddress;

    public MasterResponseOp() {
    }

    public MasterResponseOp(Address originAddress) {
        this.masterAddress = originAddress;
    }

    @Override
    public void run() {
        ClusterServiceImpl clusterService = getService();
        clusterService.getClusterJoinManager().handleMasterResponse(masterAddress, getCallerAddress());
    }

    public Address getMasterAddress() {
        return masterAddress;
    }

    @Override
    protected void readInternal(final ObjectDataInput in) throws IOException {
        masterAddress = new Address();
        masterAddress.readData(in);
    }

    @Override
    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        masterAddress.writeData(out);
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", master=").append(masterAddress);
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.MASTER_RESPONSE;
    }
}
