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

package com.hazelcast.cluster;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.logging.Level;

public class MemberRemoveOperation extends AbstractClusterOperation {

    private Address deadAddress = null;

    public MemberRemoveOperation() {
    }

    public MemberRemoveOperation(Address deadAddress) {
        super();
        this.deadAddress = deadAddress;
    }

    public void run() {
        final ClusterServiceImpl clusterService = getService();
        final Address caller = getCallerAddress();
        if (caller != null &&
                (caller.equals(deadAddress) || caller.equals(clusterService.getMasterAddress()))) {
            getLogger().finest( "Removing " + deadAddress + ", called from " + caller);
            clusterService.removeAddress(deadAddress);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        deadAddress = new Address();
        deadAddress.readData(in);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        deadAddress.writeData(out);
    }
}
