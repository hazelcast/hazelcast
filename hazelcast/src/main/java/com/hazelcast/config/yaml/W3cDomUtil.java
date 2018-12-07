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

package com.hazelcast.config.yaml;

import com.hazelcast.internal.yaml.YamlNode;
import org.w3c.dom.Node;

/**
 * Utility methods for YAML to W3C adaptors
 */
public final class W3cDomUtil {
    private W3cDomUtil() {
    }

    /**
     * Returns an adapted W3C {@link Node} view of the provided {@link YamlNode}
     *
     * @param yamlNode The YAML node to adapt
     * @return the adapted W3C Node or {@code null} if the provided YAML node is {@code null}
     */
    public static Node asW3cNode(YamlNode yamlNode) {
        if (yamlNode == null) {
            return null;
        }

        return new NodeAdapter(yamlNode);
    }
}
