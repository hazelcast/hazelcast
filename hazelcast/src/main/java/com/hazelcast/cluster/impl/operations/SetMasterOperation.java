/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster.impl.operations;

import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class SetMasterOperation extends AbstractClusterOperation implements JoinOperation {

    protected Address masterAddress;

    public SetMasterOperation() {
    }

    public SetMasterOperation(final Address originAddress) {
        this.masterAddress = originAddress;
    }

    @Override
    public void run() {
        ClusterServiceImpl clusterService = getService();
        clusterService.handleMaster(masterAddress, getCallerAddress());
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
    public String toString() {
        return "Master " + masterAddress;
    }
}
