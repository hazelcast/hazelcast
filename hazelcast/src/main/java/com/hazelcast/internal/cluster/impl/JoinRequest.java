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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.security.Credentials;
import com.hazelcast.version.MemberVersion;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readMap;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeMap;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.SetUtil.createHashSet;
import static java.util.Collections.unmodifiableSet;

public class JoinRequest extends JoinMessage {

    private Credentials credentials;
    private int tryCount;
    private Map<String, String> attributes;
    private Set<String> excludedMemberUuids = Collections.emptySet();
    // see Member.getAddressMap
    private Map<EndpointQualifier, Address> addresses;

    public JoinRequest() {
    }

    @SuppressWarnings("checkstyle:parameternumber")
    public JoinRequest(byte packetVersion, int buildNumber, MemberVersion version, Address address, String uuid,
                       boolean liteMember, ConfigCheck config, Credentials credentials, Map<String, String> attributes,
                       Set<String> excludedMemberUuids, Map<EndpointQualifier, Address> addresses) {
        super(packetVersion, buildNumber, version, address, uuid, liteMember, config);
        this.credentials = credentials;
        this.attributes = attributes;
        if (excludedMemberUuids != null) {
            this.excludedMemberUuids = unmodifiableSet(new HashSet<>(excludedMemberUuids));
        }
        this.addresses = addresses;
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

    public Set<String> getExcludedMemberUuids() {
        return excludedMemberUuids;
    }

    public MemberInfo toMemberInfo() {
        return new MemberInfo(address, uuid, attributes, liteMember, memberVersion, addresses);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        credentials = in.readObject();
        tryCount = in.readInt();
        int size = in.readInt();
        attributes = createHashMap(size);
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            String value = in.readUTF();
            attributes.put(key, value);
        }
        size = in.readInt();
        Set<String> excludedMemberUuids = createHashSet(size);
        for (int i = 0; i < size; i++) {
            excludedMemberUuids.add(in.readUTF());
        }

        this.excludedMemberUuids = unmodifiableSet(excludedMemberUuids);
        this.addresses = readMap(in);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(credentials);
        out.writeInt(tryCount);
        out.writeInt(attributes.size());
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
        out.writeInt(excludedMemberUuids.size());
        for (String uuid : excludedMemberUuids) {
            out.writeUTF(uuid);
        }
        writeMap(addresses, out);
    }

    @Override
    public String toString() {
        return "JoinRequest{"
                + "packetVersion=" + packetVersion
                + ", buildNumber=" + buildNumber
                + ", memberVersion=" + memberVersion
                + ", address=" + address
                + ", uuid='" + uuid + "'"
                + ", liteMember=" + liteMember
                + ", credentials=" + credentials
                + ", memberCount=" + getMemberCount()
                + ", tryCount=" + tryCount
                + (excludedMemberUuids.size() > 0 ? ", excludedMemberUuids=" + excludedMemberUuids : "")
                + '}';
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.JOIN_REQUEST;
    }
}
