/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.cluster.impl.operations.OnJoinOp;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.security.Credentials;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.internal.cluster.Versions.V5_3;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.readMap;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeMap;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.SetUtil.createHashSet;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public class JoinRequest extends JoinMessage {

    // RU_COMPAT 5.2
    private static final Version SUPPORTS_PREJOIN_VERSION = V5_3;
    // RU_COMPAT 5.5
    private static final MemberVersion SUPPORTS_SUPPORTED_VERSIONS_FIELD = MemberVersion.of(5, 5, 3);

    private Credentials credentials;
    private int tryCount;
    private Map<String, String> attributes;
    private Set<UUID> excludedMemberUuids = Collections.emptySet();
    // see Member.getAddressMap
    private Map<EndpointQualifier, Address> addresses;
    private UUID cpMemberUUID;
    private OnJoinOp preJoinOperation;
    private Set<Version> supportedClusterVersions = Collections.emptySet();

    public JoinRequest() {
    }

    public JoinRequest(int buildNumber, MemberVersion version, Address address, Member member,
                       ConfigCheck config, Credentials credentials,
                       Set<UUID> excludedMemberUuids, UUID cpMemberUUID,
                       OnJoinOp preJoinOperation, Set<Version> versions) {
        super(buildNumber, version, address, member.getUuid(), member.isLiteMember(), config);
        this.credentials = credentials;
        this.attributes = member.getAttributes();
        if (excludedMemberUuids != null) {
            this.excludedMemberUuids = Set.copyOf(excludedMemberUuids);
        }
        this.addresses = member.getAddressMap();
        this.cpMemberUUID = cpMemberUUID;
        this.preJoinOperation = preJoinOperation;
        this.supportedClusterVersions = requireNonNull(versions, "supportedClusterVersions must not be null");
    }

    public JoinRequest(int buildNumber, MemberVersion version, Address address, Member member, Set<Version> versions) {
        super(buildNumber, version, address, member.getUuid(), member.isLiteMember(), null);
        this.attributes = member.getAttributes();
        if (excludedMemberUuids != null) {
            this.excludedMemberUuids = Set.copyOf(excludedMemberUuids);
        }
        this.addresses = member.getAddressMap();
        this.supportedClusterVersions = requireNonNull(versions, "supportedClusterVersions must be non-null");
    }

    public Credentials getCredentials() {
        return credentials;
    }

    public int getTryCount() {
        return tryCount;
    }

    public void setTryCount(int tryCount) {
        this.tryCount = tryCount;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public Set<UUID> getExcludedMemberUuids() {
        return excludedMemberUuids;
    }

    public OnJoinOp getPreJoinOperation() {
        return preJoinOperation;
    }

    public MemberInfo toMemberInfo() {
        return new MemberInfo(address, uuid, cpMemberUUID, attributes, liteMember, memberVersion, addresses);
    }

    public boolean supportsVersion(Version version) {
        return supportedClusterVersions.contains(version);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        credentials = in.readObject();
        tryCount = in.readInt();
        int size = in.readInt();
        attributes = createHashMap(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            String value = in.readString();
            attributes.put(key, value);
        }
        size = in.readInt();
        Set<UUID> excludedMemberUuids = createHashSet(size);
        for (int i = 0; i < size; i++) {
            excludedMemberUuids.add(UUIDSerializationUtil.readUUID(in));
        }

        this.excludedMemberUuids = unmodifiableSet(excludedMemberUuids);
        this.addresses = readMap(in);
        cpMemberUUID = UUIDSerializationUtil.readUUID(in);
        // RU_COMPAT 5.2
        if (memberVersion.asVersion().isGreaterOrEqual(SUPPORTS_PREJOIN_VERSION)) {
            preJoinOperation = in.readObject();
        }
        // RU_COMPAT 5.5
        if (memberVersion.isGreaterOrEqual(SUPPORTS_SUPPORTED_VERSIONS_FIELD)) {
            int clusterVersions = in.readInt();
            this.supportedClusterVersions = createHashSet(clusterVersions);
            for (int i = 0; i < clusterVersions; i++) {
                supportedClusterVersions.add(in.readObject());
            }
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(credentials);
        out.writeInt(tryCount);
        out.writeInt(attributes.size());
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        out.writeInt(excludedMemberUuids.size());
        for (UUID uuid : excludedMemberUuids) {
            UUIDSerializationUtil.writeUUID(out, uuid);
        }
        writeMap(addresses, out);
        UUIDSerializationUtil.writeUUID(out, cpMemberUUID);
        out.writeObject(preJoinOperation);
        // no need for version checks - if we send JoinRequest to a member of older version, they will just end
        // reading before the new fields
        out.writeInt(supportedClusterVersions.size());
        for (Version version : supportedClusterVersions) {
            out.writeObject(version);
        }
    }

    @Override
    public String toString() {
        return "JoinRequest{"
                + "packetVersion=" + packetVersion
                + ", buildNumber=" + buildNumber
                + ", memberVersion=" + memberVersion
                + ", address=" + address
                + ", uuid='" + uuid + "'"
                + ", cpMemberUUID='" + cpMemberUUID + "'"
                + ", liteMember=" + liteMember
                + ", credentials=" + credentials
                + ", memberCount=" + getMemberCount()
                + ", tryCount=" + tryCount
                + (excludedMemberUuids.isEmpty() ? "" : ", excludedMemberUuids=" + excludedMemberUuids)
                + (supportedClusterVersions.isEmpty() ? "" : ", supportedClusterVersions=" + supportedClusterVersions)
                + '}';
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.JOIN_REQUEST;
    }
}
