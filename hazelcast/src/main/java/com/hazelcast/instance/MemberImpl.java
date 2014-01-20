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
import com.hazelcast.cluster.MemberAttributeChangedOperation;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.operation.MapOperationType;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class MemberImpl implements Member, HazelcastInstanceAware, IdentifiedDataSerializable {

    private static final byte TYPE_BOOLEAN = 1;
    private static final byte TYPE_BYTE = 2;
    private static final byte TYPE_SHORT = 3;
    private static final byte TYPE_INTEGER = 4;
    private static final byte TYPE_LONG = 5;
    private static final byte TYPE_FLOAT = 6;
    private static final byte TYPE_DOUBLE = 7;
    private static final byte TYPE_UTF = 8;

    private final Map<String, Object> attributes = new ConcurrentHashMap<String, Object>();

    private boolean localMember;
    private Address address;
    private String uuid;

    private transient volatile HazelcastInstanceImpl instance;

    private transient volatile long lastRead = 0;
    private transient volatile long lastWrite = 0;
    private transient volatile long lastPing = 0;
    private transient volatile ILogger logger;

    public MemberImpl() {
    }

    public MemberImpl(Address address, boolean localMember) {
        this(address, localMember, null, null);
    }

    public MemberImpl(Address address, boolean localMember, String uuid, HazelcastInstanceImpl instance) {
        this(address, localMember, uuid, instance, null);
    }

    public MemberImpl(Address address, boolean localMember, String uuid, HazelcastInstanceImpl instance, Map<String, Object> attributes) {
        this();
        this.localMember = localMember;
        this.address = address;
        this.lastRead = Clock.currentTimeMillis();
        this.uuid = uuid;
        this.instance = instance;
        if (attributes != null) this.attributes.putAll(attributes);
    }

    public MemberImpl(MemberImpl member) {
        this();
        this.localMember = member.localMember;
        this.address = member.address;
        this.lastRead = member.lastRead;
        this.uuid = member.uuid;
        this.attributes.putAll(member.attributes);
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
        return getSocketAddress();
    }

    @Override
    public InetSocketAddress getSocketAddress() {
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

    public Map<String, Object> getAttributes() {
        if (!localMember) return Collections.unmodifiableMap(attributes);
        return attributes;
    }

    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    public void updateAttribute(MapOperationType operationType, String key, Object value) {
        switch (operationType) {
            case PUT:
                attributes.put(key, value);
                break;
            case REMOVE:
                attributes.remove(key);
                break;
        }
    }

    public void setAttribute(String key, Object value) {
        if (!localMember) throw new UnsupportedOperationException("Attributes on remote members must not be changed");
        if (key == null) throw new IllegalArgumentException("key must not be null");
        if (value == null) throw new IllegalArgumentException("value must not be null");
        checkAttributeType(value);
        Object oldValue = attributes.put(key, value);
        if (value.equals(oldValue)) {
            return;
        }
        if (instance != null) {
            NodeEngineImpl nodeEngine = instance.node.nodeEngine;
            OperationService os = nodeEngine.getOperationService();
            MemberAttributeChangedOperation operation =
                    new MemberAttributeChangedOperation(MapOperationType.PUT, key, value);
            String uuid = nodeEngine.getLocalMember().getUuid();
            operation.setCallerUuid(uuid).setNodeEngine(nodeEngine);
            try {
                for (MemberImpl member : nodeEngine.getClusterService().getMemberList()) {
                    if (!member.localMember()) {
                        os.send(operation, member.getAddress());
                    } else {
                        os.executeOperation(operation);
                    }
                }
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }

    public void removeAttribute(String key) {
        if (!localMember) throw new UnsupportedOperationException("Attributes on remote members must not be changed");
        if (key == null) throw new IllegalArgumentException("key must not be null");
        Object value = attributes.remove(key);
        if (value == null) {
            return;
        }
        if (instance != null) {
            NodeEngineImpl nodeEngine = instance.node.nodeEngine;
            OperationService os = nodeEngine.getOperationService();
            MemberAttributeChangedOperation operation =
                    new MemberAttributeChangedOperation(MapOperationType.REMOVE, key, null);
            String uuid = nodeEngine.getLocalMember().getUuid();
            operation.setCallerUuid(uuid).setNodeEngine(nodeEngine);
            try {
                for (MemberImpl member : nodeEngine.getClusterService().getMemberList()) {
                    if (!member.localMember()) {
                        os.send(operation, member.getAddress());
                    } else {
                        os.executeOperation(operation);
                    }
                }
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        if (hazelcastInstance instanceof HazelcastInstanceImpl) {
            instance = (HazelcastInstanceImpl) hazelcastInstance;
            localMember = instance.node.address.equals(address);
            logger = instance.node.getLogger(this.getClass().getName());
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        address = new Address();
        address.readData(in);
        uuid = in.readUTF();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            Object value = readAttributeValue(in);
            attributes.put(key, value);
        }
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        address.writeData(out);
        out.writeUTF(uuid);
        out.writeInt(attributes.size());
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            out.writeUTF(entry.getKey());
            writeAttributeValue(entry.getValue(), out);
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

    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.MEMBER;
    }

    private void checkAttributeType(Object value) {
        Class<?> type = value.getClass();
        if (type.equals(Boolean.class) || type.equals(Byte.class)
                || type.equals(Short.class) || type.equals(Integer.class)
                || type.equals(Long.class) || type.equals(Float.class)
                || type.equals(Double.class) || type.equals(String.class)) {
            return;
        }
        throw new IllegalArgumentException("Attribute values can only have a value of types ["
                + "boolean, byte, short, int, long, float, double, String");
    }

    private void writeAttributeValue(Object value, ObjectDataOutput out) throws IOException {
        Class<?> type = value.getClass();
        if (type.equals(Boolean.class)) {
            out.writeByte(TYPE_BOOLEAN);
            out.writeBoolean((Boolean) value);
        } else if (type.equals(Byte.class)) {
            out.writeByte(TYPE_BYTE);
            out.writeByte((Byte) value);
        } else if (type.equals(Short.class)) {
            out.writeByte(TYPE_SHORT);
            out.writeShort((Short) value);
        } else if (type.equals(Integer.class)) {
            out.writeByte(TYPE_INTEGER);
            out.writeInt((Integer) value);
        } else if (type.equals(Long.class)) {
            out.writeByte(TYPE_LONG);
            out.writeLong((Long) value);
        } else if (type.equals(Float.class)) {
            out.writeByte(TYPE_FLOAT);
            out.writeFloat((Float) value);
        } else if (type.equals(Double.class)) {
            out.writeByte(TYPE_DOUBLE);
            out.writeDouble((Double) value);
        } else if(type.equals(String.class)) {
            out.writeByte(TYPE_UTF);
            out.writeUTF((String) value);
        }
    }

    private Object readAttributeValue(ObjectDataInput in) throws IOException {
        byte type = in.readByte();
        switch (type) {
            case TYPE_BOOLEAN:
                return in.readBoolean();
            case TYPE_BYTE:
                return in.readByte();
            case TYPE_SHORT:
                return in.readShort();
            case TYPE_INTEGER:
                return in.readInt();
            case TYPE_LONG:
                return in.readLong();
            case TYPE_FLOAT:
                return in.readFloat();
            case TYPE_DOUBLE:
                return in.readDouble();
            case TYPE_UTF:
                return in.readUTF();
        }
        throw new IllegalStateException("Illegal type id found");
    }

}
