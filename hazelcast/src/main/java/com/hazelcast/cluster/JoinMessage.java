/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class JoinMessage implements DataSerializable {

    protected byte packetVersion;
    protected int buildNumber;
    protected Address address;
    protected String uuid;
    protected ConfigCheck configCheck;
    protected int memberCount;

    public JoinMessage() {
    }

    public JoinMessage(byte packetVersion, int buildNumber, Address address, String uuid, ConfigCheck configCheck,
                       int memberCount) {
        this.packetVersion = packetVersion;
        this.buildNumber = buildNumber;
        this.address = address;
        this.uuid = uuid;
        this.configCheck = configCheck;
        this.memberCount = memberCount;
    }

    public byte getPacketVersion() {
        return packetVersion;
    }

    public int getBuildNumber() {
        return buildNumber;
    }

    public Address getAddress() {
        return address;
    }

    public String getUuid() {
        return uuid;
    }

    public ConfigCheck getConfigCheck() {
        return configCheck;
    }

    public int getMemberCount() {
        return memberCount;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        packetVersion = in.readByte();
        buildNumber = in.readInt();
        address = new Address();
        address.readData(in);
        uuid = in.readUTF();
        configCheck = new ConfigCheck();
        configCheck.readData(in);
        memberCount = in.readInt();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(packetVersion);
        out.writeInt(buildNumber);
        address.writeData(out);
        out.writeUTF(uuid);
        configCheck.writeData(out);
        out.writeInt(memberCount);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("JoinMessage");
        sb.append("{packetVersion=").append(packetVersion);
        sb.append(", buildNumber=").append(buildNumber);
        sb.append(", address=").append(address);
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
