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

package com.hazelcast.internal.config;

import com.hazelcast.config.MemberAttributeConfig;

import java.util.Collections;
import java.util.Map;

/**
 * Contains configuration for attribute of member (Read-Only).
 */
public class MemberAttributeConfigReadOnly extends MemberAttributeConfig {

    public MemberAttributeConfigReadOnly(MemberAttributeConfig source) {
        super(source);
    }

    @Override
    public MemberAttributeConfig setAttribute(String key, String value) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public MemberAttributeConfig removeAttribute(String key) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public MemberAttributeConfig setAttributes(Map<String, String> attributes) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public Map<String, String> getAttributes() {
        return Collections.unmodifiableMap(super.getAttributes());
    }
}
