/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class MemberImpl implements Member {

    protected boolean localMember;
    protected Address address;
    protected Node.Type nodeType;
    protected long lastRead = 0;
    protected long lastWrite = 0;

    public MemberImpl () {
    }

    public MemberImpl(Address address, boolean localMember, Node.Type nodeType) {
        super();
        this.nodeType = nodeType;
        this.localMember = localMember;
        this.address = address;
        this.lastRead = System.currentTimeMillis();
    }

    public Address getAddress() {
        return address;
    }

    public int getPort() {
        return address.getPort();
    }

    public Node.Type getNodeType() {
        return nodeType;
    }

    public InetAddress getInetAddress() {
        try {
            return address.getInetAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean localMember() {
        return localMember;
    }

    public void didWrite() {
        lastWrite = System.currentTimeMillis();
    }

    public void didRead() {
        lastRead = System.currentTimeMillis();
    }

    public long getLastRead() {
        return lastRead;
    }

    public long getLastWrite() {
        return lastWrite;
    }

    public boolean isSuperClient() {
        return (nodeType == Node.Type.SUPER_CLIENT);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Member [");
        sb.append(address.getHost());
        sb.append(":");
        sb.append(address.getPort());
        sb.append("] ");
        if (localMember) {
            sb.append("this ");
        }
        if (Node.DEBUG && address.equals(Node.get().getMasterAddress())) {
            sb.append("* ");
        }
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
        final MemberImpl other = (MemberImpl) obj;
        if (address == null) {
            if (other.address != null)
                return false;
        } else if (!address.equals(other.address))
            return false;
        return true;
    }

}
