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

package com.hazelcast.config;


import java.util.HashMap;
import java.util.Map;

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

    public String getStringAttribute(String key) {
        return (String) getAttribute(key);
    }

    public void setStringAttribute(String key, String value) {
        setAttribute(key, value);
    }

    public Boolean getBooleanAttribute(String key) {
        return (Boolean) getAttribute(key);
    }

    public void setBooleanAttribute(String key, boolean value) {
        setAttribute(key, value);
    }

    public Byte getByteAttribute(String key) {
        return (Byte) getAttribute(key);
    }

    public void setByteAttribute(String key, byte value) {
        setAttribute(key, value);
    }

    public Short getShortAttribute(String key) {
        return (Short) getAttribute(key);
    }

    public void setShortAttribute(String key, short value) {
        setAttribute(key, value);
    }

    public Integer getIntAttribute(String key) {
        return (Integer) getAttribute(key);
    }

    public void setIntAttribute(String key, int value) {
        setAttribute(key, value);
    }

    public Long getLongAttribute(String key) {
        return (Long) getAttribute(key);
    }

    public void setLongAttribute(String key, long value) {
        setAttribute(key, value);
    }

    public Float getFloatAttribute(String key) {
        return (Float) getAttribute(key);
    }

    public void setFloatAttribute(String key, float value) {
        setAttribute(key, value);
    }

    public Double getDoubleAttribute(String key) {
        return (Double) getAttribute(key);
    }

    public void setDoubleAttribute(String key, double value) {
        setAttribute(key, value);
    }

    public void removeAttribute(String key) {
        attributes.remove(key);
    }

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
