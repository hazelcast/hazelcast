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

package com.hazelcast.client.impl;

import com.hazelcast.core.Member;
import com.hazelcast.instance.AbstractMember;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.version.MemberVersion;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Map;

/**
 * Client side specific Member implementation.
 *
 * <p>Caution: This class is required by protocol encoder/decoder which are on the Hazelcast module.
 * So this implementation also stays in the same module, although it is totally client side specific.</p>
 */
@BinaryInterface
public final class MemberImpl extends AbstractMember implements Member {

    public MemberImpl() {
    }

    public MemberImpl(Address address, MemberVersion version) {
        super(address, version, null, null, false);
    }

    public MemberImpl(Address address, MemberVersion version, String uuid) {
        super(address, version, uuid, null, false);
    }

    public MemberImpl(Address address, String uuid, Map<String, Object> attributes, boolean liteMember) {
        super(address, MemberVersion.UNKNOWN, uuid, attributes, liteMember);
    }

    public MemberImpl(Address address, MemberVersion version, String uuid, Map<String, Object> attributes, boolean liteMember) {
        super(address, version, uuid, attributes, liteMember);
    }

    public MemberImpl(AbstractMember member) {
        super(member);
    }

    @Override
    protected ILogger getLogger() {
        return null;
    }

    @Override
    public boolean localMember() {
        return false;
    }

    @Override
    public String getStringAttribute(String key) {
        return (String) getAttribute(key);
    }

    @Override
    public void setStringAttribute(String key, String value) {
        throw notSupportedOnClient();
    }

    @SuppressFBWarnings(value = "NP_BOOLEAN_RETURN_NULL", justification = "null means 'missing'")
    @Override
    public Boolean getBooleanAttribute(String key) {
        Object attribute = getAttribute(key);
        return attribute != null ? Boolean.valueOf(attribute.toString()) : null;
    }

    @Override
    public void setBooleanAttribute(String key, boolean value) {
        throw notSupportedOnClient();
    }

    @Override
    public Byte getByteAttribute(String key) {
        Object attribute = getAttribute(key);
        return attribute != null ? Byte.valueOf(attribute.toString()) : null;
    }

    @Override
    public void setByteAttribute(String key, byte value) {
        throw notSupportedOnClient();
    }

    @Override
    public Short getShortAttribute(String key) {
        Object attribute = getAttribute(key);
        return attribute != null ? Short.valueOf(attribute.toString()) : null;
    }

    @Override
    public void setShortAttribute(String key, short value) {
        throw notSupportedOnClient();
    }

    @Override
    public Integer getIntAttribute(String key) {
        Object attribute = getAttribute(key);
        return attribute != null ? Integer.valueOf(attribute.toString()) : null;
    }

    @Override
    public void setIntAttribute(String key, int value) {
        throw notSupportedOnClient();
    }

    @Override
    public Long getLongAttribute(String key) {
        Object attribute = getAttribute(key);
        return attribute != null ? Long.valueOf(attribute.toString()) : null;
    }

    @Override
    public void setLongAttribute(String key, long value) {
        throw notSupportedOnClient();
    }

    @Override
    public Float getFloatAttribute(String key) {
        Object attribute = getAttribute(key);
        return attribute != null ? Float.valueOf(attribute.toString()) : null;
    }

    @Override
    public void setFloatAttribute(String key, float value) {
        throw notSupportedOnClient();
    }

    @Override
    public Double getDoubleAttribute(String key) {
        Object attribute = getAttribute(key);
        return attribute != null ? Double.valueOf(attribute.toString()) : null;
    }

    @Override
    public void setDoubleAttribute(String key, double value) {
        throw notSupportedOnClient();
    }

    @Override
    public void removeAttribute(String key) {
        throw notSupportedOnClient();
    }

    private UnsupportedOperationException notSupportedOnClient() {
        return new UnsupportedOperationException("Attributes on remote members must not be changed");
    }
}
