/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterGossipHeartbeatManager;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersViewMetadata;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

/**
 * A heartbeat sent from one cluster member to another. The sent timestamp is the cluster clock time of the sending member
 */
public final class GossipHeartbeatOp extends AbstractClusterOperation {

    private List<MembersViewMetadata> localMembersMetadata;
    private UUID targetUuid;
    private long timestamp;
    private Collection<MemberInfo> suspectedMembers;

    public GossipHeartbeatOp() {
    }

    public GossipHeartbeatOp(List<MembersViewMetadata> localMembersMetadata,
                             UUID targetUuid, long timestamp,
                             Collection<MemberInfo> suspectedMembers) {
        this.localMembersMetadata = localMembersMetadata;
        this.targetUuid = targetUuid;
        this.timestamp = timestamp;
        this.suspectedMembers = suspectedMembers;
    }

    @Override
    public void run() {
        ClusterServiceImpl service = getService();
        ClusterGossipHeartbeatManager heartbeatManager = service.getClusterGossipHeartbeatManager();
        UUID callerUuid = getCallerUuid();
        heartbeatManager.handleHeartbeat(localMembersMetadata,
                targetUuid, timestamp, suspectedMembers, callerUuid);

        getLogger().severe(String.format("callerUuid: %s, targetUuid: %s, localMembersMetadata.size: %d [%s]",
                callerUuid, targetUuid, localMembersMetadata.size(), localMembersMetadata));
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.GOSSIP_HEARTBEAT;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeInt(localMembersMetadata.size());
        for (MembersViewMetadata mvm : localMembersMetadata) {
            out.writeObject(mvm);
        }
        UUIDSerializationUtil.writeUUID(out, targetUuid);
        out.writeLong(timestamp);
        out.writeInt(suspectedMembers.size());
        for (MemberInfo m : suspectedMembers) {
            out.writeObject(m);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int mvmSize = in.readInt();
        List<MembersViewMetadata> mvmList = new ArrayList<>(mvmSize);
        for (int i = 0; i < mvmSize; i++) {
            mvmList.add(in.readObject());
        }
        localMembersMetadata = mvmList;
        targetUuid = UUIDSerializationUtil.readUUID(in);
        timestamp = in.readLong();
        int suspectedMemberCount = in.readInt();
        suspectedMembers = new HashSet<>(suspectedMemberCount);
        for (int i = 0; i < suspectedMemberCount; i++) {
            MemberInfo m = in.readObject();
            suspectedMembers.add(m);
        }
    }
}
