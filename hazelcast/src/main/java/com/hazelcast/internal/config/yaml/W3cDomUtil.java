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

package com.hazelcast.internal.config.yaml;

import com.hazelcast.internal.yaml.MutableYamlNode;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.internal.yaml.YamlSequence;
import com.hazelcast.internal.yaml.YamlUtil;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import static com.hazelcast.internal.config.yaml.EmptyNodeList.emptyNodeList;

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

        return new YamlElementAdapter(yamlNode);
    }

    /**
     * Returns the wrapped {@link YamlSequence} instance of the
     * provided {@link Node} if the {@code node} is an instance of
     * {@link YamlElementAdapter} and the YAML node wrapped by the {@code node}
     * is a {@link YamlSequence}.
     *
     * @param node The W3C node wrapping a YAML node
     * @return the wrapped YAML node as sequence
     * @throws IllegalArgumentException if the provided node is not an
     *                                  instance of {@link YamlElementAdapter}
     */
    public static YamlSequence getWrappedYamlSequence(Node node) {
        checkNodeIsElementAdapter(node);

        return asYamlType(node, YamlSequence.class);
    }

    /**
     * Returns the wrapped {@link MutableYamlNode} instance of the
     * provided {@link Node} if the {@code node} is an instance of
     * {@link YamlElementAdapter} and the YAML node wrapped by the {@code node}
     * is a {@link MutableYamlNode}.
     *
     * @param node The W3C node wrapping a YAML node
     * @return the wrapped YAML node as a mutable YAML node
     * @throws IllegalArgumentException if the provided node is not an
     *                                  instance of {@link YamlElementAdapter}
     */
    public static MutableYamlNode getWrappedMutableYamlNode(Node node) {
        checkNodeIsElementAdapter(node);

        return asYamlType(node, MutableYamlNode.class);
    }

    static NodeList asNodeList(Node node) {
        if (node == null) {
            return emptyNodeList();
        }

        return new SingletonNodeList(node);
    }

    private static <T extends YamlNode> T asYamlType(Node node, Class<T> type) {
        return YamlUtil.asType(((YamlElementAdapter) node).getYamlNode(), type);
    }

    private static void checkNodeIsElementAdapter(Node node) {
        if (!(node instanceof YamlElementAdapter)) {
            throw new IllegalArgumentException(String.format("The provided node is not an instance of ElementAdapter, it is a %s",
                    node.getClass().getName()));
        }
    }
}
