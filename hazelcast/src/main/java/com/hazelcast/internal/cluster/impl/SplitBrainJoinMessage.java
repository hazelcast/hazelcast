/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.instance.BuildInfoProvider.getBuildInfo;
import static com.hazelcast.internal.cluster.Versions.V3_10;
import static com.hazelcast.internal.cluster.Versions.V3_9;

/**
 * A {@code JoinMessage} issued by the master node of a subcluster to the master of another subcluster
 * while searching for other clusters for split brain recovery.
 */
// RU_COMPAT_39: Do not remove Versioned interface!
// Version info is needed on 3.9 members while deserializing the operation.
public class SplitBrainJoinMessage extends JoinMessage implements Versioned {

    public enum SplitBrainMergeCheckResult {
        /**
         * Denotes that the two endpoints of the SplitBrainJoinMessage cannot merge to each other
         */
        CANNOT_MERGE,
        /**
         * Denotes that the local node should merge to the other endpoint of the SplitBrainJoinMessage
         */
        LOCAL_NODE_SHOULD_MERGE,
        /**
         * Denotes that the remote node that sent the SplitBrainJoinMessage should merge
         */
        REMOTE_NODE_SHOULD_MERGE
    }

    private Version clusterVersion;

    private int memberListVersion;

    public SplitBrainJoinMessage() {
    }

    @SuppressWarnings("checkstyle:parameternumber")
    public SplitBrainJoinMessage(byte packetVersion, int buildNumber, MemberVersion version, Address address, String uuid,
                                 boolean liteMember, ConfigCheck configCheck, Collection<Address> memberAddresses,
                                 int dataMemberCount, Version clusterVersion, int memberListVersion) {
        super(packetVersion, buildNumber, version, address, uuid, liteMember, configCheck, memberAddresses, dataMemberCount);
        this.clusterVersion = clusterVersion;
        this.memberListVersion = memberListVersion;
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        clusterVersion = in.readObject();
        // - OS always includes memberListVersion
        // - messages received from 3.10+ always include the memberListVersion
        // - messages received from 3.9 EE do not include the memberListVersion
        // when serialized as part of SplitBrainMergeValidationOp (which is not Versioned in 3.9).
        // In this case, in.version is UNKNOWN
        // - messages received from 3.9 EE do include the memberListVersion when
        // serialized as standalone object. In this case in.version is 3.9
        boolean from310OrHigher = memberVersion.asVersion().isGreaterOrEqual(V3_10);
        boolean from39 = memberVersion.asVersion().equals(V3_9);
        boolean enterprise = getBuildInfo().isEnterprise();

        if (!enterprise || from310OrHigher
                || (in.getVersion().isUnknown() && !from39)
                || (in.getVersion().isGreaterOrEqual(V3_9))) {
            memberListVersion = in.readInt();
        }
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        out.writeObject(clusterVersion);
        out.writeInt(memberListVersion);
    }

    @Override
    public String toString() {
        return "SplitBrainJoinMessage{"
                + "packetVersion=" + packetVersion
                + ", buildNumber=" + buildNumber
                + ", memberVersion=" + memberVersion
                + ", clusterVersion=" + clusterVersion
                + ", address=" + address
                + ", uuid='" + uuid + '\''
                + ", liteMember=" + liteMember
                + ", memberCount=" + getMemberCount()
                + ", dataMemberCount=" + dataMemberCount
                + ", memberListVersion=" + memberListVersion
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.SPLIT_BRAIN_JOIN_MESSAGE;
    }

    public Version getClusterVersion() {
        return clusterVersion;
    }

    public int getMemberListVersion() {
        return memberListVersion;
    }
}
