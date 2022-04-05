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

package com.hazelcast.jet.cdc.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class PropertyRules {

    private final Set<String> required = new HashSet<>();
    private final Map<String, String> excludes = new HashMap<>();
    private final Map<String, String> includes = new HashMap<>();

    public PropertyRules required(String property) {
        required.add(property);
        return this;
    }

    public PropertyRules exclusive(String one, String other) {
        excludes.put(one, other);
        return this;
    }

    public PropertyRules inclusive(String one, String other) {
        includes.put(one, other);
        return this;
    }

    public void check(Properties properties) {
        for (String mandatory : required) {
            if (!properties.containsKey(mandatory)) {
                throw new IllegalStateException(mandatory + " must be specified");
            }
        }

        for (Map.Entry<String, String> entry : excludes.entrySet()) {
            if (properties.containsKey(entry.getKey()) && properties.containsKey(entry.getValue())) {
                throw new IllegalStateException(entry.getKey() + " and " + entry.getValue() + " are mutually exclusive");
            }
        }

        for (Map.Entry<String, String> entry : includes.entrySet()) {
            if (properties.contains(entry.getKey()) && !properties.contains(entry.getValue())) {
                throw new IllegalArgumentException(entry.getKey() + " requires " + entry.getValue() + " to be set too");
            }
        }
    }
}
