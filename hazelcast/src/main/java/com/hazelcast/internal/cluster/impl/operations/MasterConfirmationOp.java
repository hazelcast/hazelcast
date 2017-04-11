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
import com.hazelcast.internal.cluster.impl.MembersViewMetadata;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.internal.cluster.impl.operations.VersionedClusterOperation.isGreaterOrEqualV39;

public class MasterConfirmationOp extends AbstractClusterOperation {

    private static final String NON_AVAILABLE_UUID = "n/a";

    private MembersViewMetadata membersViewMetadata;

    private long timestamp;

    public MasterConfirmationOp() {
    }

    public MasterConfirmationOp(MembersViewMetadata membersViewMetadata, long timestamp) {
        this.membersViewMetadata = membersViewMetadata;
        this.timestamp = timestamp;
    }

    @Override
    public void run() {
        final Address endpoint = getCallerAddress();
        if (endpoint == null) {
            return;
        }

        if (membersViewMetadata == null) {
            // 3.8
            String callerUuid = getCallerUuid();
            if (callerUuid == null) {
                callerUuid = NON_AVAILABLE_UUID;
            }
            membersViewMetadata = new MembersViewMetadata(endpoint, callerUuid, getNodeEngine().getThisAddress(), 0);
        }

        final ClusterServiceImpl clusterService = getService();
        clusterService.handleMasterConfirmation(membersViewMetadata, timestamp);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        if (isGreaterOrEqualV39(out.getVersion())) {
            out.writeObject(membersViewMetadata);
        }
        out.writeLong(timestamp);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        if (isGreaterOrEqualV39(in.getVersion())) {
            membersViewMetadata = in.readObject();
        }
        timestamp = in.readLong();
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.MASTER_CONFIRM;
    }
}
