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

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.version.Version;

import java.io.IOException;

/** A heartbeat sent from one cluster member to another. The sent timestamp is the cluster clock time of the sending member */
public final class HeartbeatOp extends VersionedClusterOperation {

    private String targetUuid;
    private long timestamp;

    public HeartbeatOp() {
        super(0);
    }

    public HeartbeatOp(String targetUuid, int version, long timestamp) {
        super(version);
        this.targetUuid = targetUuid;
        this.timestamp = timestamp;
    }

    @Override
    public void run() {
        ClusterServiceImpl service = getService();
        if (!wasSentToThisMember(service)) {
            getLogger().warning("Heartbeat was sent to " + targetUuid + ", but this is not our UUID anymore.");
            return;
        }

        MemberImpl member = getHeartBeatingMember(service);
        if (member != null) {
            checkMemberListVersion(service);
            service.getClusterHeartbeatManager().onHeartbeat(member, timestamp);
        }
    }

    private boolean wasSentToThisMember(ClusterServiceImpl service) {
        Version clusterVersion = service.getClusterVersion();
        if (clusterVersion.isUnknown() || clusterVersion.isLessThan(Versions.V3_9)) {
            return true;
        }
        
        Member localMember = service.getLocalMember();
        return localMember.getUuid().equals(targetUuid);
    }

    private void checkMemberListVersion(ClusterServiceImpl service) {
        Version clusterVersion = service.getClusterVersion();
        if (clusterVersion.isUnknown() || clusterVersion.isLessThan(Versions.V3_9)) {
            return;
        }

        int localMemberListVersion = service.getMembershipManager().getMemberListVersion();

        ILogger logger = getLogger();
        if (service.isMaster()) {
            if (localMemberListVersion < getMemberListVersion()) {
                logger.severe("Master member list version must not be smaller than sender's version! Master: "
                        + localMemberListVersion + ", Sender: " + getMemberListVersion());
            }

            if (localMemberListVersion - getMemberListVersion() > 1) {
                logger.warning("Sender member list version is lagging behind master's version! "
                        + "Master: " + localMemberListVersion + ", Sender: " + getMemberListVersion());
            }

        } else if (getCallerAddress().equals(service.getMasterAddress())) {
            if (localMemberListVersion > getMemberListVersion()) {
                logger.severe("Local member list version must not be greater than master's version! Master: "
                        + getMemberListVersion() + ", Local: " + localMemberListVersion);
            }

            if (getMemberListVersion() - localMemberListVersion > 1) {
                logger.warning("Local member list version is lagging behind master's version! "
                        + "Master: " + getMemberListVersion() + ", Local: " + localMemberListVersion);
            }
        }
    }

    // TODO [basri] If I am the master and I receive a heartbeat from a non-member address, I can tell it to suspect me
    private MemberImpl getHeartBeatingMember(ClusterServiceImpl service) {
        MemberImpl member = service.getMember(getCallerAddress());
        ILogger logger = getLogger();
        if (member == null) {
            if (logger.isFineEnabled()) {
                logger.fine("Heartbeat received from an unknown endpoint: " + getCallerAddress());
            }
            return null;
        }

        Version clusterVersion = service.getClusterVersion();
        if (clusterVersion.isUnknown() || clusterVersion.isLessThan(Versions.V3_9)) {
            // uuid is not send explicitly pre-3.9
            return member;
        }

        if (!member.getUuid().equals(getCallerUuid())) {
            if (logger.isFineEnabled()) {
                logger.fine("Heartbeat received from an unknown endpoint. Address: "
                        + getCallerAddress() + ", Uuid: " + getCallerUuid());
            }
            return null;
        }
        return member;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.HEARTBEAT;
    }

    @Override
    void writeInternalImpl(ObjectDataOutput out) throws IOException {
        if (isGreaterOrEqual_V3_9(out.getVersion())) {
            out.writeUTF(targetUuid);
        }

        out.writeLong(timestamp);
    }

    @Override
    void readInternalImpl(ObjectDataInput in) throws IOException {
        if (isGreaterOrEqual_V3_9(in.getVersion())) {
            targetUuid = in.readUTF();
        }

        timestamp = in.readLong();
    }
}
