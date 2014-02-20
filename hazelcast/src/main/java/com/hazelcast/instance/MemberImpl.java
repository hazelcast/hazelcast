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

package com.hazelcast.instance;

import com.hazelcast.cluster.ClusterDataSerializerHook;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.MemberAttributes;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public final class MemberImpl implements Member, HazelcastInstanceAware, IdentifiedDataSerializable {

    private boolean localMember;
    private Address address;
    private String uuid;
    private MemberAttributes memberAttributes;

    private transient volatile long lastRead = 0;
    private transient volatile long lastWrite = 0;
    private transient volatile long lastPing = 0;
    private transient volatile ILogger logger;

    public MemberImpl() {
    }

    public MemberImpl(Address address, boolean localMember) {
        this(address, localMember, null, null);
    }

    public MemberImpl(Address address, boolean localMember, String uuid) {
        this(address, localMember, uuid, null);
    }

    public MemberImpl(Address address, boolean localMember, MemberAttributes memberAttributes) {
        this(address, localMember, null, memberAttributes);
    }

    public MemberImpl(Address address, boolean localMember, String uuid, MemberAttributes memberAttributes) {
        this();
        this.localMember = localMember;
        this.address = address;
        this.lastRead = Clock.currentTimeMillis();
        this.uuid = uuid;
        this.memberAttributes = memberAttributes;
    }


    public Address getAddress() {
        return address;
    }

    public int getPort() {
        return address.getPort();
    }

    public InetAddress getInetAddress() {
        try {
            return address.getInetAddress();
        } catch (UnknownHostException e) {
            if (logger != null) {
                logger.warning(e);
            }
            return null;
        }
    }

    public InetSocketAddress getInetSocketAddress() {
        try {
            return address.getInetSocketAddress();
        } catch (UnknownHostException e) {
            if (logger != null) {
                logger.warning(e);
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

    void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getUuid() {
        return uuid;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        if (hazelcastInstance instanceof HazelcastInstanceImpl) {
            HazelcastInstanceImpl instance = (HazelcastInstanceImpl) hazelcastInstance;
            localMember = instance.node.address.equals(address);
            logger = instance.node.getLogger(this.getClass().getName());
        }
    }

    @Override
    public MemberAttributes getMemberAttributes() {
        return memberAttributes;
    }

    public void readData(ObjectDataInput in) throws IOException {
        address = new Address();
        address.readData(in);
        uuid = in.readUTF();
        if (in.readBoolean()) {
            memberAttributes = new MemberAttributes();
            memberAttributes.readData(in);
        }
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        address.writeData(out);
        out.writeUTF(uuid);
        out.writeBoolean(memberAttributes != null);
        if (null != memberAttributes) {
            memberAttributes.writeData(out);
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
        return sb.toString();
    }

    @Override
    public int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + ((address == null) ? 0 : address.hashCode());
        result = PRIME * result + ((memberAttributes == null) ? 0 : memberAttributes.hashCode());
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
        if (memberAttributes == null) {
            if (other.memberAttributes != null)
                return false;
        } else if (!memberAttributes.equals(other.memberAttributes)) {
            return false;
        }
        return true;
    }

    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.MEMBER;
    }
}
