/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.internal.config.override;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A representation of a single generic configuration node.
 */
class ConfigNode {
    private final ConfigNode parent;
    private final String name;
    private final Map<String, ConfigNode> children = new LinkedHashMap<>();
    private String value;

    ConfigNode(String name) {
        this.name = name;
        this.parent = null;
    }

    ConfigNode(String name, ConfigNode parent) {
        this.name = name;
        this.parent = parent;
    }

    String getName() {
        return name;
    }

    Optional<String> getValue() {
        return Optional.ofNullable(value);
    }

    void setValue(String value) {
        this.value = value;
    }

    Map<String, ConfigNode> getChildren() {
        return children;
    }

    Optional<ConfigNode> getParent() {
        return Optional.ofNullable(parent);
    }
}
