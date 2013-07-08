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
import com.hazelcast.cluster.ClusterServiceImpl;
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
import com.hazelcast.spi.InvocationBuilder;
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
import java.util.Map.Entry;
import java.util.logging.Level;

public final class MemberImpl implements Member, HazelcastInstanceAware, IdentifiedDataSerializable {

	private final Map<String, Object> attributes = new HashMap<String, Object>();
	
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
        this(address, localMember, null, null, null);
    }

    public MemberImpl(Address address, boolean localMember, String uuid, HazelcastInstance instance, Map<String, Object> attributes) {
        this();
        this.localMember = localMember;
        this.address = address;
        this.lastRead = Clock.currentTimeMillis();
        this.uuid = uuid;
        this.instance = (HazelcastInstanceImpl) instance;
    	if (attributes != null) this.attributes.putAll(attributes);
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

	public Map<String, Object> getAttributes() {
		if (!localMember) return Collections.unmodifiableMap(attributes);
		return attributes;
	}

	public Object getAttribute(String key) {
		return attributes.get(key);
	}

	public void setAttribute(String key, Object value) {
		if (!localMember) throw new UnsupportedOperationException("Attributes on remote members must not be changed");
		if (value != null) {
			attributes.put(key, value);
		} else {
			attributes.remove(key);
		}
		if (instance != null) {
			NodeEngineImpl nodeEngine = instance.node.nodeEngine;
			OperationService os = nodeEngine.getOperationService();
			MemberAttributeChangedOperation operation;
			if (value != null) {
				operation = new MemberAttributeChangedOperation(MapOperationType.PUT, key, value);
			} else {
				operation = new MemberAttributeChangedOperation(MapOperationType.REMOVE, key, null);
			}
			String uuid = nodeEngine.getLocalMember().getUuid();
			operation.setCallerUuid(uuid).setNodeEngine(nodeEngine);
			try {
				for (Member m : nodeEngine.getClusterService().getMembers()) {
					MemberImpl member = (MemberImpl) m;
					InvocationBuilder inv = os.createInvocationBuilder(ClusterServiceImpl.SERVICE_NAME, operation, member.getAddress());
					inv.build().invoke();
				}
			} catch (Throwable t) {
	            throw ExceptionUtil.rethrow(t);
			}
		}
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
			Object value = in.readObject();
			attributes.put(key, value);
		}
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        address.writeData(out);
        out.writeUTF(uuid);
		out.writeInt(attributes.size());
		for (Entry<String, Object> entry : attributes.entrySet()) {
			out.writeUTF(entry.getKey());
			out.writeObject(entry.getValue());
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
}
