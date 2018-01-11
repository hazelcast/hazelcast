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

import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.version.MemberVersion;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

public class SimpleMemberImpl implements Member {

    private String uuid;
    private InetSocketAddress address;
    private boolean liteMember;
    private MemberVersion version;

    @SuppressWarnings("unused")
    public SimpleMemberImpl() {
    }

    public SimpleMemberImpl(MemberVersion version, String uuid, InetSocketAddress address) {
        this(version, uuid, address, false);
    }

    public SimpleMemberImpl(MemberVersion version, String uuid, InetSocketAddress address, boolean liteMember) {
        this.version = version;
        this.uuid = uuid;
        this.address = address;
        this.liteMember = liteMember;
    }

    @Override
    public Address getAddress() {
        return new Address(address);
    }

    @Override
    public boolean localMember() {
        return false;
    }

    @Override
    public InetSocketAddress getInetSocketAddress() {
        return getSocketAddress();
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return address;
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
        return null;
    }

    @Override
    public String getStringAttribute(String key) {
        return null;
    }

    @Override
    public void setStringAttribute(String key, String value) {
    }

    @Override
    public Boolean getBooleanAttribute(String key) {
        return null;
    }

    @Override
    public void setBooleanAttribute(String key, boolean value) {
    }

    @Override
    public Byte getByteAttribute(String key) {
        return null;
    }

    @Override
    public void setByteAttribute(String key, byte value) {
    }

    @Override
    public Short getShortAttribute(String key) {
        return null;
    }

    @Override
    public void setShortAttribute(String key, short value) {
    }

    @Override
    public Integer getIntAttribute(String key) {
        return null;
    }

    @Override
    public void setIntAttribute(String key, int value) {
    }

    @Override
    public Long getLongAttribute(String key) {
        return null;
    }

    @Override
    public void setLongAttribute(String key, long value) {
    }

    @Override
    public Float getFloatAttribute(String key) {
        return null;
    }

    @Override
    public void setFloatAttribute(String key, float value) {
    }

    @Override
    public Double getDoubleAttribute(String key) {
        return null;
    }

    @Override
    public void setDoubleAttribute(String key, double value) {
    }

    @Override
    public void removeAttribute(String key) {
    }

    @Override
    public MemberVersion getVersion() {
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(version);
        out.writeUTF(uuid);
        out.writeObject(address);
        out.writeBoolean(liteMember);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        version = in.readObject();
        uuid = in.readUTF();
        address = in.readObject();
        liteMember = in.readBoolean();
    }
}
