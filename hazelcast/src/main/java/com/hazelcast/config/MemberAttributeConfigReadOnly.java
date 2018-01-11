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

package com.hazelcast.config;

import java.util.Collections;
import java.util.Map;

/**
 * Contains configuration for attribute of member (Read-Only).
 *
 * @deprecated this class will be removed in 4.0; it is meant for internal usage only.
 */
public class MemberAttributeConfigReadOnly extends MemberAttributeConfig {

    MemberAttributeConfigReadOnly(MemberAttributeConfig source) {
        super(source);
    }

    @Override
    public void setStringAttribute(String key, String value) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setBooleanAttribute(String key, boolean value) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setByteAttribute(String key, byte value) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setShortAttribute(String key, short value) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setIntAttribute(String key, int value) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setLongAttribute(String key, long value) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setFloatAttribute(String key, float value) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setDoubleAttribute(String key, double value) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void removeAttribute(String key) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setAttributes(Map<String, Object> attributes) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public Map<String, Object> getAttributes() {
        return Collections.unmodifiableMap(super.getAttributes());
    }
}
