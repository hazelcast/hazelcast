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

import com.hazelcast.config.GroupConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import com.hazelcast.wan.impl.WanDataSerializerHook;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

import static com.hazelcast.internal.cluster.impl.operations.WanProtocolNegotiationStatus.GROUP_NAME_MISMATCH;
import static com.hazelcast.internal.cluster.impl.operations.WanProtocolNegotiationStatus.OK;
import static com.hazelcast.internal.cluster.impl.operations.WanProtocolNegotiationStatus.PROTOCOL_MISMATCH;

/**
 * WAN protocol negotiation operation sent from source to target cluster to
 * negotiate WAN protocol and return metadata about target member and
 * cluster.
 */
public class WanProtocolNegotiationOperation extends Operation implements JoinOperation {
    private Collection<Version> sourceWanProtocolVersions;
    private String sourceGroupName;
    private String targetGroupName;
    private WanProtocolNegotiationResponse response;

    public WanProtocolNegotiationOperation() {
    }

    public WanProtocolNegotiationOperation(String sourceGroupName,
                                           String targetGroupName,
                                           Collection<Version> sourceWanProtocolVersions) {
        this.sourceGroupName = sourceGroupName;
        this.targetGroupName = targetGroupName;
        this.sourceWanProtocolVersions = sourceWanProtocolVersions;
    }

    @Override
    public void run() {
        MemberVersion memberVersion = getNodeEngine().getLocalMember().getVersion();
        Version clusterVersion = getNodeEngine().getClusterService().getClusterVersion();
        Map<String, String> metadata = Collections.emptyMap();

        GroupConfig groupConfig = getNodeEngine().getConfig().getGroupConfig();
        if (!targetGroupName.equals(groupConfig.getName())) {
            String failureCause = String.format(
                    "WAN protocol negotiation from (%s,%s) failed because of group name mismatch. ",
                    sourceGroupName, getCallerAddress());
            getLogger().info(failureCause);
            response = new WanProtocolNegotiationResponse(GROUP_NAME_MISMATCH, memberVersion, clusterVersion, null, metadata);
            return;
        }

        Collection<Version> localProtocolVersions = getNodeEngine().getWanReplicationService()
                                                                   .getSupportedWanProtocolVersions();
        Optional<Version> chosenProtocolVersion = localProtocolVersions.stream()
                                                                       .filter(sourceWanProtocolVersions::contains)
                                                                       .max(Comparator.naturalOrder());

        if (!chosenProtocolVersion.isPresent()) {
            String failureCause = String.format(
                    "WAN protocol negotiation from (%s , %s) failed because no matching WAN protocol versions were found. "
                            + "Source member supports %s, target supports %s",
                    sourceGroupName, getCallerAddress(), sourceWanProtocolVersions, localProtocolVersions);
            getLogger().info(failureCause);
            response = new WanProtocolNegotiationResponse(PROTOCOL_MISMATCH, memberVersion, clusterVersion, null, metadata);
            return;
        }

        response = new WanProtocolNegotiationResponse(OK, memberVersion, clusterVersion, chosenProtocolVersion.get(), metadata);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        sourceGroupName = in.readUTF();
        targetGroupName = in.readUTF();
        int sourceWanProtocolVersionSize = in.readInt();
        sourceWanProtocolVersions = new ArrayList<>(sourceWanProtocolVersionSize);
        for (int i = 0; i < sourceWanProtocolVersionSize; i++) {
            sourceWanProtocolVersions.add(in.readObject());
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(sourceGroupName);
        out.writeUTF(targetGroupName);
        out.writeInt(sourceWanProtocolVersions.size());
        for (Version version : sourceWanProtocolVersions) {
            out.writeObject(version);
        }
    }


    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.WAN_PROTOCOL_NEGOTIATION_OPERATION;
    }
}
