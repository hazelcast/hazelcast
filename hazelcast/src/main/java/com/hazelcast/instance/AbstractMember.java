/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.version.MemberVersion;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@PrivateApi
@BinaryInterface
public abstract class AbstractMember implements Member {

    protected final Map<String, Object> attributes = new ConcurrentHashMap<String, Object>();
    protected Address address;
    protected String uuid;
    protected boolean liteMember;
    protected MemberVersion version;

    protected AbstractMember() {
    }

    protected AbstractMember(Address address, MemberVersion version, String uuid, Map<String, Object> attributes,
                             boolean liteMember) {
        assert address != null : "Address is required!";
        this.address = address;
        this.version = version;
        this.uuid = uuid != null ? uuid : "<" + address.toString() + ">";
        if (attributes != null) {
            this.attributes.putAll(attributes);
        }
        this.liteMember = liteMember;
    }

    protected AbstractMember(AbstractMember member) {
        this.address = member.address;
        this.version = member.version;
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
    public MemberVersion getVersion() {
        return version;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        address = new Address();
        address.readData(in);
        uuid = in.readUTF();
        liteMember = in.readBoolean();
        version = in.readObject();
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
        out.writeObject(version);
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
        sb.append(" - ").append(uuid);
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
        int result = address.hashCode();
        result = 31 * result + uuid.hashCode();
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
        if (!(obj instanceof Member)) {
            return false;
        }

        Member that = (Member) obj;
        return address.equals(that.getAddress()) && uuid.equals(that.getUuid());
    }
}
