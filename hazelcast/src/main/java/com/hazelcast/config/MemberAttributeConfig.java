/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;


import java.util.HashMap;
import java.util.Map;

/**
 * Contains configuration for attribute of member.
 */
public class MemberAttributeConfig {

    private final Map<String, Object> attributes = new HashMap<String, Object>();

    public MemberAttributeConfig() {
    }

    public MemberAttributeConfig(MemberAttributeConfig source) {
        attributes.putAll(source.attributes);
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public MemberAttributeConfig setAttributes(Map<String, Object> attributes) {
        this.attributes.clear();
        if (attributes != null) {
            this.attributes.putAll(attributes);
        }
        return this;
    }

    public String getStringAttribute(String key) {
        return (String) getAttribute(key);
    }

    public MemberAttributeConfig setStringAttribute(String key, String value) {
        setAttribute(key, value);
        return this;
    }

    public Boolean getBooleanAttribute(String key) {
        return (Boolean) getAttribute(key);
    }

    public MemberAttributeConfig setBooleanAttribute(String key, boolean value) {
        setAttribute(key, value);
        return this;
    }

    public Byte getByteAttribute(String key) {
        return (Byte) getAttribute(key);
    }

    public MemberAttributeConfig setByteAttribute(String key, byte value) {
        setAttribute(key, value);
        return this;
    }

    public Short getShortAttribute(String key) {
        return (Short) getAttribute(key);
    }

    public MemberAttributeConfig setShortAttribute(String key, short value) {
        setAttribute(key, value);
        return this;
    }

    public Integer getIntAttribute(String key) {
        return (Integer) getAttribute(key);
    }

    public MemberAttributeConfig setIntAttribute(String key, int value) {
        setAttribute(key, value);
        return this;
    }

    public Long getLongAttribute(String key) {
        return (Long) getAttribute(key);
    }

    public MemberAttributeConfig setLongAttribute(String key, long value) {
        setAttribute(key, value);
        return this;
    }

    public Float getFloatAttribute(String key) {
        return (Float) getAttribute(key);
    }

    public MemberAttributeConfig setFloatAttribute(String key, float value) {
        setAttribute(key, value);
        return this;
    }

    public Double getDoubleAttribute(String key) {
        return (Double) getAttribute(key);
    }

    public MemberAttributeConfig setDoubleAttribute(String key, double value) {
        setAttribute(key, value);
        return this;
    }

    public MemberAttributeConfig removeAttribute(String key) {
        attributes.remove(key);
        return this;
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public MemberAttributeConfig asReadOnly() {
        return new MemberAttributeConfigReadOnly(this);
    }

    private Object getAttribute(String key) {
        return attributes.get(key);
    }

    private void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }
}
