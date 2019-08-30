/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.List;

public class HashIndexConfig extends IndexConfig {

    private List<String> attributes;

    public HashIndexConfig() {
        // No-op.
    }

    public HashIndexConfig(String attribute) {
        addAttribute(attribute);
    }

    public List<String> getAttributes() {
        if (attributes == null)
            attributes = new ArrayList<>();

        return attributes;
    }

    public HashIndexConfig addAttribute(String attribute) {
        getAttributes().add(attribute);

        return this;
    }

    public HashIndexConfig setAttributes(List<String> attributes) {
        this.attributes = attributes;
        return this;
    }

    @Override
    public String toString() {
        return "HashIndexConfig{name=" + name + ", attributes=" + attributes + '}';
    }
}
