package com.hazelcast.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import com.hazelcast.impl.NodeType;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;

public class Member implements com.hazelcast.core.Member, DataSerializable{

	private static final long serialVersionUID = 670282750752162302L;
	private Address address;
	private NodeType nodeType;

	public InetAddress getInetAddress() {
		try {
			return address.getInetAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		}
	}

	public InetSocketAddress getInetSocketAddress() {
		try {
			return address.getInetSocketAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		}
	}

	public int getPort() {
		return address.getPort();
	}

	public boolean isSuperClient() {
		// TODO Auto-generated method stub
		return NodeType.SUPER_CLIENT.equals(this.nodeType);
	}

	public boolean localMember() {
		return false;
	}

	public void readData(DataInput in) throws IOException {
        address = new Address();
        address.readData(in);
        nodeType = NodeType.create(in.readInt());
        String factoryName = in.readUTF();
    }

    public void writeData(DataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

}
