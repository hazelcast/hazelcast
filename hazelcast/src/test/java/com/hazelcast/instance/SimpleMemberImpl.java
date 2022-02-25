/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.version.MemberVersion;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class SimpleMemberImpl implements Member {

    private UUID uuid;
    private InetSocketAddress address;
    private boolean liteMember;
    private MemberVersion version;

    @SuppressWarnings("unused")
    public SimpleMemberImpl() {
    }

    public SimpleMemberImpl(MemberVersion version, UUID uuid, InetSocketAddress address) {
        this(version, uuid, address, false);
    }

    public SimpleMemberImpl(MemberVersion version, UUID uuid, InetSocketAddress address, boolean liteMember) {
        this.version = version;
        this.uuid = uuid;
        this.address = address;
        this.liteMember = liteMember;
    }

    @Override
    public Address getAddress() {
        return new Address(address);
    }

    @Override
    public Map<EndpointQualifier, Address> getAddressMap() {
        return Collections.singletonMap(EndpointQualifier.MEMBER, new Address(address));
    }

    @Override
    public boolean localMember() {
        return false;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return address;
    }

    @Override
    public InetSocketAddress getSocketAddress(EndpointQualifier qualifier) {
        return address;
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public boolean isLiteMember() {
        return liteMember;
    }

    @Override
    public Map<String, String> getAttributes() {
        return null;
    }

    @Override
    public String getAttribute(String key) {
        return null;
    }

    @Override
    public MemberVersion getVersion() {
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(version);
        UUIDSerializationUtil.writeUUID(out, uuid);
        out.writeObject(address);
        out.writeBoolean(liteMember);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        version = in.readObject();
        uuid = UUIDSerializationUtil.readUUID(in);
        address = in.readObject();
        liteMember = in.readBoolean();
    }
}
