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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.version.MemberVersion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

// Used as request and response in join protocol
public class JoinMessage implements IdentifiedDataSerializable {

    protected byte packetVersion;
    protected int buildNumber;
    /**
     * this is populated with the codebase version of the node trying to join the cluster
     * (ie {@link com.hazelcast.instance.Node#getVersion()}).
     */
    protected MemberVersion memberVersion;
    protected Address address;
    protected String uuid;
    protected boolean liteMember;
    protected ConfigCheck configCheck;
    protected Collection<Address> memberAddresses;
    protected int dataMemberCount;

    public JoinMessage() {
    }

    public JoinMessage(byte packetVersion, int buildNumber, MemberVersion memberVersion, Address address,
                       String uuid, boolean liteMember, ConfigCheck configCheck) {
        this(packetVersion, buildNumber, memberVersion, address, uuid, liteMember, configCheck,
                Collections.<Address>emptySet(), 0);
    }

    public JoinMessage(byte packetVersion, int buildNumber, MemberVersion memberVersion, Address address, String uuid,
                       boolean liteMember, ConfigCheck configCheck, Collection<Address> memberAddresses, int dataMemberCount) {
        this.packetVersion = packetVersion;
        this.buildNumber = buildNumber;
        this.memberVersion = memberVersion;
        this.address = address;
        this.uuid = uuid;
        this.liteMember = liteMember;
        this.configCheck = configCheck;
        this.memberAddresses = memberAddresses;
        this.dataMemberCount = dataMemberCount;
    }

    public byte getPacketVersion() {
        return packetVersion;
    }

    public int getBuildNumber() {
        return buildNumber;
    }

    public MemberVersion getMemberVersion() {
        return memberVersion;
    }

    public Address getAddress() {
        return address;
    }

    public String getUuid() {
        return uuid;
    }

    public boolean isLiteMember() {
        return liteMember;
    }

    public ConfigCheck getConfigCheck() {
        return configCheck;
    }

    public int getMemberCount() {
        return memberAddresses != null ? memberAddresses.size() : 0;
    }

    public Collection<Address> getMemberAddresses() {
        return memberAddresses != null ? memberAddresses : Collections.<Address>emptySet();
    }

    public int getDataMemberCount() {
        return dataMemberCount;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        packetVersion = in.readByte();
        buildNumber = in.readInt();
        memberVersion = in.readObject();
        address = new Address();
        address.readData(in);
        uuid = in.readUTF();
        configCheck = new ConfigCheck();
        configCheck.readData(in);
        liteMember = in.readBoolean();

        int memberCount = in.readInt();
        memberAddresses = new ArrayList<Address>(memberCount);
        for (int i = 0; i < memberCount; i++) {
            Address member = new Address();
            member.readData(in);
            memberAddresses.add(member);
        }
        dataMemberCount = in.readInt();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(packetVersion);
        out.writeInt(buildNumber);
        out.writeObject(memberVersion);
        address.writeData(out);
        out.writeUTF(uuid);
        configCheck.writeData(out);
        out.writeBoolean(liteMember);

        int memberCount = getMemberCount();
        out.writeInt(memberCount);
        if (memberCount > 0) {
            for (Address member : memberAddresses) {
                member.writeData(out);
            }
        }
        out.writeInt(dataMemberCount);
    }

    @Override
    public String toString() {
        return "JoinMessage{"
                + "packetVersion=" + packetVersion
                + ", buildNumber=" + buildNumber
                + ", memberVersion=" + memberVersion
                + ", address=" + address
                + ", uuid='" + uuid + '\''
                + ", liteMember=" + liteMember
                + ", memberCount=" + getMemberCount()
                + ", dataMemberCount=" + dataMemberCount
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.JOIN_MESSAGE;
    }
}
