/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

public class MasterConfirmationOp extends VersionedClusterOperation {

    private long timestamp;

    public MasterConfirmationOp() {
        super(0);
    }

    public MasterConfirmationOp(int memberListVersion, long timestamp) {
        super(memberListVersion);
        this.timestamp = timestamp;
    }

    @Override
    public void run() {
        final Address endpoint = getCallerAddress();
        if (endpoint == null) {
            return;
        }

        final ClusterServiceImpl clusterService = getService();
        clusterService.handleMasterConfirmation(endpoint, getMemberListVersion(), timestamp);
    }

    @Override
    void writeInternalImpl(ObjectDataOutput out)
            throws IOException {
        out.writeLong(timestamp);
    }

    @Override
    void readInternalImpl(ObjectDataInput in)
            throws IOException {
        timestamp = in.readLong();
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.MASTER_CONFIRM;
    }
}
