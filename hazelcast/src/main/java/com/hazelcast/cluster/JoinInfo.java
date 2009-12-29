/**
 * 
 */
package com.hazelcast.cluster;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;

import com.hazelcast.impl.NodeType;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;

public class JoinInfo extends JoinRequest {

	private static final long serialVersionUID = 1088129500826234941L;
	private boolean request = true;

    public JoinInfo() {
    }

    public JoinInfo(boolean request, Address address, String groupName, String groupPassword,
                    NodeType type, byte packetVersion, int buildNumber) {
        super(address, groupName, groupPassword, type, packetVersion, buildNumber);
        this.setRequest(request);
    }

    public JoinInfo copy(boolean newRequest, Address newAddress) {
        return new JoinInfo(newRequest, newAddress, groupName, groupPassword, nodeType, packetVersion, buildNumber);
    }

    public void writeToPacket(DatagramPacket packet) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        try {
            dos.writeBoolean(isRequest());
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

    public void readFromPacket(DatagramPacket packet) {
        ByteArrayInputStream bis = new ByteArrayInputStream(packet.getData(), 0, packet
                .getLength());
        DataInputStream dis = new DataInputStream(bis);
        try {
            setRequest(dis.readBoolean());
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
                "request=" + isRequest() + "  " + super.toString() +
                '}';
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
}