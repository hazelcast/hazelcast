/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
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

    public PropertyRules required(String property) {
        required.add(property);
        return this;
    }

    public PropertyRules exclusive(String one, String other) {
        excludes.put(one, other);
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
    }
}
