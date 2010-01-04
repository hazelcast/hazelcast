/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.cluster;

import com.hazelcast.impl.NodeType;
import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinRequest extends AbstractRemotelyProcessable {

    protected NodeType nodeType = NodeType.MEMBER;
    public Address address;
    public Address to;
    public String groupName;
    public String groupPassword;
    public byte packetVersion;
    public int buildNumber;

    public JoinRequest() {
        super();
    }

    public JoinRequest(Address address, String groupName, String groupPassword, NodeType type, byte packetVersion, int buildNumber) {
        this(null, address, groupName, groupPassword, type, packetVersion, buildNumber);
    }

    public JoinRequest(Address to, Address address, String groupName, String groupPassword, NodeType type, byte packetVersion, int buildNumber) {
        super();
        this.to = to;
        this.address = address;
        this.groupName = groupName;
        this.groupPassword = groupPassword;
        this.nodeType = type;
        this.packetVersion = packetVersion;
        this.buildNumber = buildNumber;
    }

    @Override
    public void readData(DataInput in) throws IOException {
        boolean hasTo = in.readBoolean();
        if (hasTo) {
            to = new Address();
            to.readData(in);
        }
        address = new Address();
        address.readData(in);
        nodeType = NodeType.create(in.readInt());
        groupName = in.readUTF();
        groupPassword = in.readUTF();
        packetVersion = in.readByte();
        buildNumber = in.readInt();
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        boolean hasTo = (to != null);
        out.writeBoolean(hasTo);
        if (hasTo) {
            to.writeData(out);
        }
        address.writeData(out);
        out.writeInt(nodeType.getValue());
        out.writeUTF(groupName);
        out.writeUTF(groupPassword);
        out.writeByte(packetVersion);
        out.writeInt(buildNumber);
    }

    @Override
    public String toString() {
        return new StringBuilder(128)
                .append("JoinRequest{")
                .append("nodeType=").append(nodeType)
                .append(", address=").append(address)
                .append(", groupName='").append(groupName).append('\'')
                .append(", groupPassword='").append(groupPassword).append('\'')
                .append(", buildNumber='").append(buildNumber).append('\'')
                .append(", packetVersion='").append(packetVersion).append('\'')
                .append('}')
                .toString();
    }

    public void process() {
        getNode().clusterManager.handleJoinRequest(this);
    }
}
