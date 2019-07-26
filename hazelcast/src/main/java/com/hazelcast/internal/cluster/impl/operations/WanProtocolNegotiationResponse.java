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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.MapUtil;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import com.hazelcast.wan.impl.WanDataSerializerHook;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Response for WAN protocol negotiation from a WAN replication target.
 */
public class WanProtocolNegotiationResponse implements IdentifiedDataSerializable {
    private WanProtocolNegotiationStatus status;
    private MemberVersion memberVersion;
    private Version clusterVersion;
    private Version chosenWanProtocolVersion;
    private Map<String, String> metadata;

    @SuppressWarnings("unused")
    public WanProtocolNegotiationResponse() {
    }

    public WanProtocolNegotiationResponse(WanProtocolNegotiationStatus status,
                                          MemberVersion memberVersion,
                                          Version clusterVersion,
                                          Version chosenWanProtocolVersion,
                                          Map<String, String> metadata) {
        this.status = status;
        this.memberVersion = memberVersion;
        this.clusterVersion = clusterVersion;
        this.chosenWanProtocolVersion = chosenWanProtocolVersion;
        this.metadata = metadata;
    }

    /**
     * Returns the protocol negotiation status.
     */
    public WanProtocolNegotiationStatus getStatus() {
        return status;
    }

    /**
     * Returns the chosen protocol version.
     */
    public Version getChosenWanProtocolVersion() {
        return chosenWanProtocolVersion;
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.WAN_PROTOCOL_NEGOTIATION_RESPONSE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(status.getStatusCode());
        out.writeObject(memberVersion);
        out.writeObject(clusterVersion);
        out.writeObject(chosenWanProtocolVersion);
        out.writeInt(metadata.size());
        for (Entry<String, String> entry : metadata.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        status = WanProtocolNegotiationStatus.getByType(in.readByte());
        memberVersion = in.readObject();
        clusterVersion = in.readObject();
        chosenWanProtocolVersion = in.readObject();
        int metadataSize = in.readInt();
        metadata = MapUtil.createHashMap(metadataSize);
        for (int i = 0; i < metadataSize; i++) {
            metadata.put(in.readUTF(), in.readUTF());
        }
    }
}
