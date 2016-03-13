/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractMember implements Member {

    protected final Map<String, Object> attributes = new ConcurrentHashMap<String, Object>();
    protected Address address;
    protected String uuid;
    protected boolean liteMember;

    protected AbstractMember() {
    }

    protected AbstractMember(Address address) {
        this(address, null, null);
    }

    protected AbstractMember(Address address, String uuid) {
        this(address, uuid, null);
    }

    protected AbstractMember(Address address, String uuid, Map<String, Object> attributes) {
        this(address, uuid, attributes, false);
    }

    protected AbstractMember(Address address, String uuid, Map<String, Object> attributes, boolean liteMember) {
        this.address = address;
        this.uuid = uuid;
        if (attributes != null) {
            this.attributes.putAll(attributes);
        }
        this.liteMember = liteMember;
    }

    protected AbstractMember(AbstractMember member) {
        this.address = member.address;
        this.uuid = member.uuid;
        this.attributes.putAll(member.attributes);
        this.liteMember = member.liteMember;
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
            if (getLogger() != null) {
                getLogger().warning(e);
            }
            return null;
        }
    }

    protected abstract ILogger getLogger();

    @Override
    public InetSocketAddress getInetSocketAddress() {
        return getSocketAddress();
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        try {
            return address.getInetSocketAddress();
        } catch (UnknownHostException e) {
            if (getLogger() != null) {
                getLogger().warning(e);
            }
            return null;
        }
    }

    void setUuid(String uuid) {
        this.uuid = uuid;
    }

    @Override
    public String getUuid() {
        return uuid;
    }

    @Override
    public boolean isLiteMember() {
        return liteMember;
    }

    @Override
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

    protected Object getAttribute(String key) {
        return attributes.get(key);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        address = new Address();
        address.readData(in);
        uuid = in.readUTF();
        liteMember = in.readBoolean();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            Object value = IOUtil.readAttributeValue(in);
            attributes.put(key, value);
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        address.writeData(out);
        out.writeUTF(uuid);
        out.writeBoolean(liteMember);
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
        if (localMember()) {
            sb.append(" this");
        }
        if (isLiteMember()) {
            sb.append(" lite");
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((address == null) ? 0 : address.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof AbstractMember)) {
            return false;
        }
        final AbstractMember other = (AbstractMember) obj;
        if (address == null) {
            if (other.address != null) {
                return false;
            }
        } else if (!address.equals(other.address)) {
            return false;
        }
        return true;
    }
}
