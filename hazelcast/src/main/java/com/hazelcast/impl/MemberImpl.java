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

package com.hazelcast.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.util.Clock;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;

public final class MemberImpl implements Member, HazelcastInstanceAware, DataSerializable {

    protected boolean localMember;
    protected Address address;
    protected NodeType nodeType;
    protected String uuid;
    protected transient long lastRead = 0;
    protected transient long lastWrite = 0;
    @SuppressWarnings("VolatileLongOrDoubleField")
    protected transient volatile long lastPing = 0;
    private volatile transient ILogger logger;

    public MemberImpl() {
    }

    public MemberImpl(Address address, boolean localMember) {
        this(address, localMember, NodeType.MEMBER, null);
    }

    public MemberImpl(Address address, boolean localMember, NodeType nodeType, String uuid) {
        this();
        this.nodeType = nodeType;
        this.localMember = localMember;
        this.address = address;
        this.lastRead = Clock.currentTimeMillis();
        this.uuid = uuid;
    }

    public Address getAddress() {
        return address;
    }

    public int getPort() {
        return address.getPort();
    }

    public NodeType getNodeType() {
        return nodeType;
    }

    public InetAddress getInetAddress() {
        try {
            return address.getInetAddress();
        } catch (UnknownHostException e) {
            if (logger != null) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
            return null;
        }
    }

    public InetSocketAddress getInetSocketAddress() {
        try {
            return address.getInetSocketAddress();
        } catch (UnknownHostException e) {
            if (logger != null) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
            return null;
        }
    }

    public boolean localMember() {
        return localMember;
    }

    public void didWrite() {
        lastWrite = Clock.currentTimeMillis();
    }

    public void didRead() {
        lastRead = Clock.currentTimeMillis();
    }

    public void didPing() {
        lastPing = Clock.currentTimeMillis();
    }

    public long getLastPing() {
        return lastPing;
    }

    public long getLastRead() {
        return lastRead;
    }

    public long getLastWrite() {
        return lastWrite;
    }

    @Deprecated
    public boolean isSuperClient() {
        return isLiteMember();
    }

    public boolean isLiteMember() {
        return (nodeType == NodeType.LITE_MEMBER);
    }

    void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getUuid() {
        return uuid;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        FactoryImpl factoryImpl = (FactoryImpl) hazelcastInstance;
        localMember = factoryImpl.node.address.equals(address);
        logger = factoryImpl.node.getLogger(this.getClass().getName());
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        writeData(out);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        readData(in);
    }

    public void readData(DataInput in) throws IOException {
        address = new Address();
        address.readData(in);
        nodeType = NodeType.create(in.readInt());
        lastRead = Clock.currentTimeMillis();
        if (in.readBoolean()) {
            uuid = in.readUTF();
        }
    }

    public void writeData(DataOutput out) throws IOException {
        address.writeData(out);
        out.writeInt(nodeType.getValue());
        boolean hasUuid = uuid != null;
        out.writeBoolean(hasUuid);
        if (hasUuid) {
            out.writeUTF(uuid);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Member [");
        sb.append(address.getHost());
        sb.append("]");
        sb.append(":");
        sb.append(address.getPort());
        if (localMember) {
            sb.append(" this");
        }
        if (nodeType == NodeType.LITE_MEMBER) {
            sb.append(" lite");
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
