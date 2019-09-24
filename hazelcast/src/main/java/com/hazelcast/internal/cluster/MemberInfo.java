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

package com.hazelcast.internal.cluster;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.version.MemberVersion;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.cluster.impl.MemberImpl.NA_MEMBER_LIST_JOIN_VERSION;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.readMap;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeMap;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static java.util.Collections.singletonMap;

public class MemberInfo implements IdentifiedDataSerializable {

    private Address address;
    private String uuid;
    private boolean liteMember;
    private MemberVersion version;
    private Map<String, String> attributes;
    private int memberListJoinVersion = NA_MEMBER_LIST_JOIN_VERSION;
    // since 3.12
    private Map<EndpointQualifier, Address> addressMap;

    public MemberInfo() {
    }

    public MemberInfo(Address address, String uuid, Map<String, String> attributes, MemberVersion version) {
        this(address, uuid, attributes, false, version, NA_MEMBER_LIST_JOIN_VERSION, Collections.emptyMap());
    }

    public MemberInfo(Address address, String uuid, Map<String, String> attributes, boolean liteMember, MemberVersion version,
                      Map<EndpointQualifier, Address> addressMap) {
        this(address, uuid, attributes, liteMember, version, NA_MEMBER_LIST_JOIN_VERSION, addressMap);
    }

    public MemberInfo(Address address, String uuid, Map<String, String> attributes, boolean liteMember, MemberVersion version,
                      int memberListJoinVersion, Map<EndpointQualifier, Address> addressMap) {
        this.address = address;
        this.uuid = uuid;
        this.attributes = attributes == null || attributes.isEmpty() ? Collections.emptyMap() : new HashMap<>(attributes);
        this.liteMember = liteMember;
        this.version = version;
        this.memberListJoinVersion = memberListJoinVersion;
        this.addressMap = addressMap;
    }

    public MemberInfo(MemberImpl member) {
        this(member.getAddress(), member.getUuid(), member.getAttributes(), member.isLiteMember(), member.getVersion(),
                member.getMemberListJoinVersion(), member.getAddressMap());
    }

    public Address getAddress() {
        return address;
    }

    public MemberVersion getVersion() {
        return version;
    }

    public String getUuid() {
        return uuid;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public boolean isLiteMember() {
        return liteMember;
    }

    public int getMemberListJoinVersion() {
        return memberListJoinVersion;
    }

    public Map<EndpointQualifier, Address> getAddressMap() {
        return addressMap;
    }

    public MemberImpl toMember() {
        return new MemberImpl.Builder(singletonMap(MEMBER, address))
                .version(version)
                .uuid(uuid)
                .attributes(attributes)
                .liteMember(liteMember)
                .memberListJoinVersion(memberListJoinVersion)
                .build();
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        address = in.readObject();
        uuid = in.readUTF();
        liteMember = in.readBoolean();
        int size = in.readInt();
        if (size > 0) {
            attributes = createHashMap(size);
        }
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            String value = in.readUTF();
            attributes.put(key, value);
        }
        version = in.readObject();
        memberListJoinVersion = in.readInt();
        addressMap = readMap(in);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(address);
        out.writeUTF(uuid);
        out.writeBoolean(liteMember);
        out.writeInt(attributes == null ? 0 : attributes.size());
        if (attributes != null) {
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
        }
        out.writeObject(version);
        out.writeInt(memberListJoinVersion);
        writeMap(addressMap, out);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((address == null) ? 0 : address.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        MemberInfo other = (MemberInfo) obj;
        if (address == null) {
            return other.address == null;
        } else {
            return address.equals(other.address);
        }
    }

    @Override
    public String toString() {
        return "MemberInfo{"
                + "address=" + address
                + ", uuid=" + uuid
                + ", liteMember=" + liteMember
                + ", memberListJoinVersion=" + memberListJoinVersion
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.MEMBER_INFO;
    }
}
