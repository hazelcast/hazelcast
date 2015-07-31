/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Map;

/**
 * Client side specific Member implementation.
 *
 * <p>Caution: This class is required by protocol encoder/decoder which are on hazelcast module
 * so this impl also stays in the same module, although it is totally client side specific</p>
 */
public final class MemberImpl
        extends AbstractMember
        implements Member {

    public MemberImpl() {
    }

    public MemberImpl(Address address) {
        super(address);
    }

    public MemberImpl(Address address, String uuid) {
        super(address, uuid);
    }

    public MemberImpl(Address address, String uuid, Map<String, Object> attributes) {
        super(address, uuid, attributes);
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
        notSupportedOnClient();
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings("NP_BOOLEAN_RETURN_NULL")
    @Override
    public Boolean getBooleanAttribute(String key) {
        final Object attribute = getAttribute(key);
        return attribute != null ? Boolean.valueOf(attribute.toString()) : null;
    }

    @Override
    public void setBooleanAttribute(String key, boolean value) {
        notSupportedOnClient();
    }

    @Override
    public Byte getByteAttribute(String key) {
        final Object attribute = getAttribute(key);
        return attribute != null ? Byte.valueOf(attribute.toString()) : null;
    }

    @Override
    public void setByteAttribute(String key, byte value) {
        notSupportedOnClient();
    }

    @Override
    public Short getShortAttribute(String key) {
        final Object attribute = getAttribute(key);
        return attribute != null ? Short.valueOf(attribute.toString()) : null;
    }

    @Override
    public void setShortAttribute(String key, short value) {
        notSupportedOnClient();
    }

    @Override
    public Integer getIntAttribute(String key) {
        final Object attribute = getAttribute(key);
        return attribute != null ? Integer.valueOf(attribute.toString()) : null;
    }

    @Override
    public void setIntAttribute(String key, int value) {
        notSupportedOnClient();
    }

    @Override
    public Long getLongAttribute(String key) {
        final Object attribute = getAttribute(key);
        return attribute != null ? Long.getLong(attribute.toString()) : null;
    }

    @Override
    public void setLongAttribute(String key, long value) {
        notSupportedOnClient();
    }

    @Override
    public Float getFloatAttribute(String key) {
        final Object attribute = getAttribute(key);
        return attribute != null ? Float.valueOf(attribute.toString()) : null;
    }

    @Override
    public void setFloatAttribute(String key, float value) {
        notSupportedOnClient();
    }

    @Override
    public Double getDoubleAttribute(String key) {
        final Object attribute = getAttribute(key);
        return attribute != null ? Double.valueOf(attribute.toString()) : null;
    }

    @Override
    public void setDoubleAttribute(String key, double value) {
        notSupportedOnClient();
    }

    @Override
    public void removeAttribute(String key) {
        notSupportedOnClient();
    }

    private void notSupportedOnClient() {
        throw new UnsupportedOperationException("Attributes on remote members must not be changed");
    }

}
