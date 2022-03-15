/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import java.util.Objects;

/**
 * Contains configuration for attribute of member.
 */
public class MemberAttributeConfig {

    private final Map<String, String> attributes = new HashMap<>();

    public MemberAttributeConfig() {
    }

    public MemberAttributeConfig(MemberAttributeConfig source) {
        attributes.putAll(source.attributes);
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public MemberAttributeConfig setAttributes(Map<String, String> attributes) {
        this.attributes.clear();
        if (attributes != null) {
            this.attributes.putAll(attributes);
        }
        return this;
    }

    public String getAttribute(String key) {
        return attributes.get(key);
    }

    public MemberAttributeConfig setAttribute(String key, String value) {
        this.attributes.put(key, value);
        return this;
    }

    public MemberAttributeConfig removeAttribute(String key) {
        attributes.remove(key);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MemberAttributeConfig that = (MemberAttributeConfig) o;
        return Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributes);
    }
}
