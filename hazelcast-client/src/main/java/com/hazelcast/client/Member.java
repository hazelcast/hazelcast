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
    }

    public void writeData(DataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Member [");
        sb.append(address.getHost());
        sb.append(":");
        sb.append(address.getPort());
        sb.append("] ");
        return sb.toString();
    }

    @Override
    public int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + ((address == null) ? 0 : address.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final Member other = (Member) obj;
        if (address == null) {
            if (other.address != null)
                return false;
        } else if (!address.equals(other.address))
            return false;
        return true;
    }
}
