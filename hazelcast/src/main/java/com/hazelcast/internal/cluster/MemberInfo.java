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

package com.hazelcast.internal.cluster;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.version.MemberVersion;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.instance.MemberImpl.NA_MEMBER_LIST_JOIN_VERSION;
import static com.hazelcast.internal.cluster.Versions.V3_10;
import static com.hazelcast.util.MapUtil.createHashMap;

public class MemberInfo implements IdentifiedDataSerializable, Versioned {

    private Address address;
    private String uuid;
    private boolean liteMember;
    private MemberVersion version;
    private Map<String, Object> attributes;
    private int memberListJoinVersion = NA_MEMBER_LIST_JOIN_VERSION;

    public MemberInfo() {
    }

    public MemberInfo(Address address, String uuid, Map<String, Object> attributes, MemberVersion version) {
        this(address, uuid, attributes, false, version, NA_MEMBER_LIST_JOIN_VERSION);
    }

    public MemberInfo(Address address, String uuid, Map<String, Object> attributes, boolean liteMember, MemberVersion version) {
        this(address, uuid, attributes, liteMember, version, NA_MEMBER_LIST_JOIN_VERSION);
    }

    public MemberInfo(Address address, String uuid, Map<String, Object> attributes, boolean liteMember, MemberVersion version,
                      int memberListJoinVersion) {
        this.address = address;
        this.uuid = uuid;
        this.attributes = attributes == null || attributes.isEmpty()
                ? Collections.<String, Object>emptyMap() : new HashMap<String, Object>(attributes);
        this.liteMember = liteMember;
        this.version = version;
        this.memberListJoinVersion = memberListJoinVersion;
    }

    public MemberInfo(MemberImpl member) {
        this(member.getAddress(), member.getUuid(), member.getAttributes(), member.isLiteMember(), member.getVersion(),
                member.getMemberListJoinVersion());
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

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public boolean isLiteMember() {
        return liteMember;
    }

    public int getMemberListJoinVersion() {
        return memberListJoinVersion;
    }

    public MemberImpl toMember() {
        return new MemberImpl(address, version, false, uuid, attributes, liteMember, memberListJoinVersion, null);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        address = new Address();
        address.readData(in);
        if (in.readBoolean()) {
            uuid = in.readUTF();
        }
        liteMember = in.readBoolean();
        int size = in.readInt();
        if (size > 0) {
            attributes = createHashMap(size);
        }
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            Object value = in.readObject();
            attributes.put(key, value);
        }
        version = in.readObject();
        // RU_COMPAT_3_9
        if (in.getVersion().isGreaterOrEqual(V3_10)) {
            memberListJoinVersion = in.readInt();
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        address.writeData(out);
        boolean hasUuid = uuid != null;
        out.writeBoolean(hasUuid);
        if (hasUuid) {
            out.writeUTF(uuid);
        }
        out.writeBoolean(liteMember);
        out.writeInt(attributes == null ? 0 : attributes.size());
        if (attributes != null) {
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeObject(entry.getValue());
            }
        }
        out.writeObject(version);
        // RU_COMPAT_3_9
        if (out.getVersion().isGreaterOrEqual(V3_10)) {
            out.writeInt(memberListJoinVersion);
        }
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
            if (other.address != null) {
                return false;
            }
        } else if (!address.equals(other.address)) {
            return false;
        }
        return true;
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
    public int getId() {
        return ClusterDataSerializerHook.MEMBER_INFO;
    }
}
