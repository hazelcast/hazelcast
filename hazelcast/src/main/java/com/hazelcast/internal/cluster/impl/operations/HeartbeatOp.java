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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterHeartbeatManager;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersViewMetadata;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;

/** A heartbeat sent from one cluster member to another. The sent timestamp is the cluster clock time of the sending member */
// RU_COMPAT_39: Do not remove Versioned interface!
// Version info is needed on 3.9 members while deserializing the operation.
public final class HeartbeatOp extends AbstractClusterOperation implements Versioned {

    private MembersViewMetadata senderMembersViewMetadata;
    private String targetUuid;
    private long timestamp;

    public HeartbeatOp() {
    }

    public HeartbeatOp(MembersViewMetadata senderMembersViewMetadata, String targetUuid, long timestamp) {
        this.senderMembersViewMetadata = senderMembersViewMetadata;
        this.targetUuid = targetUuid;
        this.timestamp = timestamp;
    }

    @Override
    public void run() {
        ClusterServiceImpl service = getService();
        ClusterHeartbeatManager heartbeatManager = service.getClusterHeartbeatManager();
        heartbeatManager.handleHeartbeat(senderMembersViewMetadata, targetUuid, timestamp);
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.HEARTBEAT;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(senderMembersViewMetadata);
        out.writeUTF(targetUuid);
        out.writeLong(timestamp);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        senderMembersViewMetadata = in.readObject();
        targetUuid = in.readUTF();
        timestamp = in.readLong();
    }

}
