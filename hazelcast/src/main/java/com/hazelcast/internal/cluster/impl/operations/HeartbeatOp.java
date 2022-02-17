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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterHeartbeatManager;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersViewMetadata;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;

/** A heartbeat sent from one cluster member to another. The sent timestamp is the cluster clock time of the sending member */
public final class HeartbeatOp extends AbstractClusterOperation implements Versioned {

    private MembersViewMetadata senderMembersViewMetadata;
    private UUID targetUuid;
    private long timestamp;
    private Collection<MemberInfo> suspectedMembers;

    public HeartbeatOp() {
    }

    public HeartbeatOp(MembersViewMetadata senderMembersViewMetadata, UUID targetUuid, long timestamp,
                       Collection<MemberInfo> suspectedMembers) {
        this.senderMembersViewMetadata = senderMembersViewMetadata;
        this.targetUuid = targetUuid;
        this.timestamp = timestamp;
        this.suspectedMembers = suspectedMembers;
    }

    @Override
    public void run() {
        ClusterServiceImpl service = getService();
        ClusterHeartbeatManager heartbeatManager = service.getClusterHeartbeatManager();
        heartbeatManager.handleHeartbeat(senderMembersViewMetadata, targetUuid, timestamp, suspectedMembers);
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.HEARTBEAT;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(senderMembersViewMetadata);
        UUIDSerializationUtil.writeUUID(out, targetUuid);
        out.writeLong(timestamp);
        if (out.getVersion().isGreaterThan(Versions.V4_0)) {
            out.writeInt(suspectedMembers.size());
            for (MemberInfo m : suspectedMembers) {
                out.writeObject(m);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        senderMembersViewMetadata = in.readObject();
        targetUuid = UUIDSerializationUtil.readUUID(in);
        timestamp = in.readLong();
        if (in.getVersion().isGreaterThan(Versions.V4_0)) {
            int suspectedMemberCount = in.readInt();
            suspectedMembers = new HashSet<>(suspectedMemberCount);
            for (int i = 0; i < suspectedMemberCount; i++) {
                MemberInfo m = in.readObject();
                suspectedMembers.add(m);
            }
        } else {
            suspectedMembers = Collections.emptySet();
        }
    }

}
