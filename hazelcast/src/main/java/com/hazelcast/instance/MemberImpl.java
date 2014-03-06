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
import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.cluster.MemberAttributeOperationType.PUT;
import static com.hazelcast.cluster.MemberAttributeOperationType.REMOVE;

public final class MemberImpl implements Member, HazelcastInstanceAware, IdentifiedDataSerializable {

    private final Map<String, Object> attributes = new ConcurrentHashMap<String, Object>();

    private boolean localMember;
    private Address address;
    private String uuid;

    private volatile HazelcastInstanceImpl instance;

    private volatile long lastRead = 0;
    private volatile long lastWrite = 0;
    private volatile long lastPing = 0;
    private volatile ILogger logger;

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
        return Collections.unmodifiableMap(attributes);
    }

    public void updateAttribute(MemberAttributeOperationType operationType, String key, Object value) {
        switch (operationType) {
            case PUT:
                attributes.put(key, value);
                break;
            case REMOVE:
                attributes.remove(key);
                break;
            default:
                throw new IllegalArgumentException("Not a known OperationType " + operationType);
        }
    }

    @Override
    public String getStringAttribute(String key) {
        return (String) getAttribute(key);
    }

    @Override
    public void setStringAttribute(String key, String value) {
        setAttribute(key, value);
    }

    @Override
    public Boolean getBooleanAttribute(String key) {
        return (Boolean) getAttribute(key);
    }

    @Override
    public void setBooleanAttribute(String key, boolean value) {
        setAttribute(key, value);
    }

    @Override
    public Byte getByteAttribute(String key) {
        return (Byte) getAttribute(key);
    }

    @Override
    public void setByteAttribute(String key, byte value) {
        setAttribute(key, value);
    }

    @Override
    public Short getShortAttribute(String key) {
        return (Short) getAttribute(key);
    }

    @Override
    public void setShortAttribute(String key, short value) {
        setAttribute(key, value);
    }

    @Override
    public Integer getIntAttribute(String key) {
        return (Integer) getAttribute(key);
    }

    @Override
    public void setIntAttribute(String key, int value) {
        setAttribute(key, value);
    }

    @Override
    public Long getLongAttribute(String key) {
        return (Long) getAttribute(key);
    }

    @Override
    public void setLongAttribute(String key, long value) {
        setAttribute(key, value);
    }

    @Override
    public Float getFloatAttribute(String key) {
        return (Float) getAttribute(key);
    }

    @Override
    public void setFloatAttribute(String key, float value) {
        setAttribute(key, value);
    }

    @Override
    public Double getDoubleAttribute(String key) {
        return (Double) getAttribute(key);
    }

    @Override
    public void setDoubleAttribute(String key, double value) {
        setAttribute(key, value);
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
                    new MemberAttributeChangedOperation(REMOVE, key, null);
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
            Object value = IOUtil.readAttributeValue(in);
            attributes.put(key, value);
        }
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        address.writeData(out);
        out.writeUTF(uuid);
        Map<String, Object> attributes = new HashMap<String, Object>(this.attributes);
        out.writeInt(attributes.size());
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            out.writeUTF(entry.getKey());
            IOUtil.writeAttributeValue(entry.getValue(), out);
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

    private Object getAttribute(String key) {
        return attributes.get(key);
    }

    private void setAttribute(String key, Object value) {
        if (!localMember) throw new UnsupportedOperationException("Attributes on remote members must not be changed");
        if (key == null) throw new IllegalArgumentException("key must not be null");
        if (value == null) throw new IllegalArgumentException("value must not be null");
        Object oldValue = attributes.put(key, value);
        if (value.equals(oldValue)) {
            return;
        }
        if (instance != null) {
            NodeEngineImpl nodeEngine = instance.node.nodeEngine;
            OperationService os = nodeEngine.getOperationService();
            MemberAttributeChangedOperation operation =
                    new MemberAttributeChangedOperation(PUT, key, value);
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

}
