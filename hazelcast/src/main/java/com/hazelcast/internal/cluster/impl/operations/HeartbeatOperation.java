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
public final class HeartbeatOperation extends VersionedClusterOperation {

    private long timestamp;

    public HeartbeatOperation() {
        super(0);
    }

    public HeartbeatOperation(int version, long timestamp) {
        super(version);
        this.timestamp = timestamp;
    }

    @Override
    public void run() {
        ClusterServiceImpl service = getService();
        MemberImpl member = getHeartBeatingMember(service);
        if (member != null) {
            checkMemberListVersion();
            service.getClusterHeartbeatManager().onHeartbeat(member, timestamp);
        }
    }

    private void checkMemberListVersion() {
        ClusterServiceImpl service = getService();
        Version clusterVersion = service.getClusterVersion();
        if (clusterVersion.isUnknown() || clusterVersion.isLessThan(Versions.V3_9)) {
            return;
        }

        int localMemberListVersion = service.getMemberListVersion();

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

    private MemberImpl getHeartBeatingMember(ClusterServiceImpl service) {
        MemberImpl member = service.getMember(getCallerAddress());
        ILogger logger = getLogger();
        if (member == null) {
            if (logger.isFineEnabled()) {
                logger.fine("Heartbeat received from an unknown endpoint: " + getCallerAddress());
            }
            return null;
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
        out.writeLong(timestamp);
    }

    @Override
    void readInternalImpl(ObjectDataInput in) throws IOException {
        timestamp = in.readLong();
    }
}
