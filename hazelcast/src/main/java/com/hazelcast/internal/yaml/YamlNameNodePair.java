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

package com.hazelcast.internal.yaml;

/**
 * A pair consists of a node's name and its representing {@link YamlNode}
 * instance
 *
 * @see YamlMapping#childrenPairs()
 */
public class YamlNameNodePair {
    private final String nodeName;
    private final YamlNode childNode;

    YamlNameNodePair(String nodeName, YamlNode childNode) {
        this.nodeName = nodeName;
        this.childNode = childNode;
    }

    /**
     * Returns the name of the node
     *
     * @return the name of the node
     */
    public String nodeName() {
        return nodeName;
    }

    /**
     * The {@link YamlNode} instance
     *
     * @return the node instance if present or {@code null} if the node
     * is explicitly defined as {@code !!null} in the YAML document
     */
    public YamlNode childNode() {
        return childNode;
    }
}
