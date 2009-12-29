/**
 * 
 */
package com.hazelcast.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;

import com.hazelcast.cluster.JoinRequest;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;

public class JoinInfo extends JoinRequest {

	private static final long serialVersionUID = 1088129500826234941L;
	boolean request = true;

    public JoinInfo() {
    }

    public JoinInfo(boolean request, Address address, String groupName, String groupPassword,
                    NodeType type, byte packetVersion, int buildNumber) {
        super(address, groupName, groupPassword, type, packetVersion, buildNumber);
        this.request = request;
    }

    public JoinInfo copy(boolean newRequest, Address newAddress) {
        return new JoinInfo(newRequest, newAddress, groupName, groupPassword, nodeType, packetVersion, buildNumber);
    }

    void writeToPacket(DatagramPacket packet) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        try {
            dos.writeBoolean(request);
            address.writeData(dos);
            dos.writeUTF(groupName);
            dos.writeUTF(groupPassword);
            dos.writeByte(Packet.PACKET_VERSION);
            dos.writeInt(buildNumber);
            packet.setData(bos.toByteArray());
            packet.setLength(bos.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void readFromPacket(DatagramPacket packet) {
        ByteArrayInputStream bis = new ByteArrayInputStream(packet.getData(), 0, packet
                .getLength());
        DataInputStream dis = new DataInputStream(bis);
        try {
            request = dis.readBoolean();
            address = new Address();
            address.readData(dis);
            groupName = dis.readUTF();
            groupPassword = dis.readUTF();
            packetVersion = dis.readByte();
            buildNumber = dis.readInt();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "JoinInfo{" +
                "request=" + request + "  " + super.toString() +
                '}';
    }
}