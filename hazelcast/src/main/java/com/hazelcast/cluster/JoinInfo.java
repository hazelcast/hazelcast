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
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.Packet;

import java.io.*;
import java.net.DatagramPacket;

public class JoinInfo extends JoinRequest implements DataSerializable {

    private static final long serialVersionUID = 1088129500826234941L;
    private boolean request = true;
    private int memberCount = 0;

    public JoinInfo() {
    }

    public JoinInfo(boolean request, Address address, String groupName, String groupPassword,
                    NodeType type, byte packetVersion, int buildNumber, int memberCount) {
        super(address, groupName, groupPassword, type, packetVersion, buildNumber);
        this.setRequest(request);
        this.memberCount = memberCount;
    }

    public JoinInfo copy(boolean newRequest, Address newAddress) {
        return new JoinInfo(newRequest, newAddress, groupName, groupPassword,
                nodeType, packetVersion, buildNumber, memberCount);
    }

    @Override
    public void readData(DataInput dis) throws IOException {
        setRequest(dis.readBoolean());
        address = new Address();
        address.readData(dis);
        groupName = dis.readUTF();
        groupPassword = dis.readUTF();
        packetVersion = dis.readByte();
        buildNumber = dis.readInt();
        memberCount = dis.readInt();
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        try {
            out.writeBoolean(isRequest());
            address.writeData(out);
            out.writeUTF(groupName);
            out.writeUTF(groupPassword);
            out.writeByte(Packet.PACKET_VERSION);
            out.writeInt(buildNumber);
            out.writeInt(memberCount);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeToPacket(DatagramPacket packet) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        try {
            writeData(dos);
            packet.setData(bos.toByteArray());
            packet.setLength(bos.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readFromPacket(DatagramPacket packet) {
        ByteArrayInputStream bis = new ByteArrayInputStream(packet.getData(), 0, packet.getLength());
        DataInputStream dis = new DataInputStream(bis);
        try {
            readData(dis);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param request the request to set
     */
    public void setRequest(boolean request) {
        this.request = request;
    }

    /**
     * @return the request
     */
    public boolean isRequest() {
        return request;
    }

    public int getMemberCount() {
        return memberCount;
    }

    @Override
    public String toString() {
        return "JoinInfo{" +
                "request=" + isRequest() +
                ", memberCount=" + memberCount +
                "  " + super.toString() +
                '}';
    }
}
