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
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class MemberRemoveOperation extends AbstractClusterOperation {

    private Address deadAddress;

    public MemberRemoveOperation() {
    }

    public MemberRemoveOperation(Address deadAddress) {
        this.deadAddress = deadAddress;
    }

    @Override
    public void run() {
        final ClusterServiceImpl clusterService = getService();
        final Address caller = getCallerAddress();
        if (caller != null
                && (caller.equals(deadAddress) || caller.equals(clusterService.getMasterAddress()))) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Removing " + deadAddress + ", called from " + caller);
            }
            clusterService.removeAddress(deadAddress);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        deadAddress = new Address();
        deadAddress.readData(in);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        deadAddress.writeData(out);
    }
}
