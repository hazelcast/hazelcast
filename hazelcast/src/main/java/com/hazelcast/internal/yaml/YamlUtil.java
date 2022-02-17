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

import java.util.regex.Pattern;

/**
 * Utility class for working with YAML nodes.
 */
public final class YamlUtil {
    private static final Pattern NAME_APOSTROPHE_PATTERN = Pattern.compile("(.*[\\t\\s,;:]+.*)");

    private YamlUtil() {
    }

    /**
     * Takes a generic {@link YamlNode} instance and returns it casted to
     * {@link YamlMapping} if the type of the node is a descendant of
     * {@link YamlMapping}.
     *
     * @param node The generic node to cast
     * @return the casted mapping
     * @throws YamlException if the provided node is not a mapping
     */
    public static YamlMapping asMapping(YamlNode node) {
        if (node != null && !(node instanceof YamlMapping)) {
            String nodeName = node.nodeName();
            throw new YamlException(String.format("Child %s is not a mapping, it's actual type is %s",
                    nodeName, node.getClass()));
        }

        return (YamlMapping) node;
    }

    /**
     * Takes a generic {@link YamlNode} instance and returns it casted to
     * {@link YamlSequence} if the type of the node is a descendant of
     * {@link YamlSequence}.
     *
     * @param node The generic node to cast
     * @return the casted sequence
     * @throws YamlException if the provided node is not a sequence
     */
    public static YamlSequence asSequence(YamlNode node) {
        if (node != null && !(node instanceof YamlSequence)) {
            String nodeName = node.nodeName();
            throw new YamlException(String.format("Child %s is not a sequence, it's actual type is %s",
                    nodeName, node.getClass()));
        }

        return (YamlSequence) node;
    }

    /**
     * Takes a generic {@link YamlNode} instance and returns it casted to
     * {@link YamlScalar} if the type of the node is a descendant of
     * {@link YamlScalar}.
     *
     * @param node The generic node to cast
     * @return the casted scalar
     * @throws YamlException if the provided node is not a scalar
     */
    public static YamlScalar asScalar(YamlNode node) {
        if (node != null && !(node instanceof YamlScalar)) {
            String nodeName = node.nodeName();
            throw new YamlException(String.format("Child %s is not a scalar, it's actual type is %s", nodeName, node.getClass()));
        }

        return (YamlScalar) node;
    }

    /**
     * Takes a generic {@link YamlNode} instance and returns it casted to
     * the provided {@code type} if the node is an instance of that type.
     *
     * @param node The generic node to cast
     * @return the casted node
     * @throws YamlException if the provided node is not the expected type
     */
    @SuppressWarnings("unchecked")
    public static <T> T asType(YamlNode node, Class<T> type) {
        if (node != null && !type.isAssignableFrom(node.getClass())) {
            String nodeName = node.nodeName();
            throw new YamlException(String.format("Child %s is not a %s, it's actual type is %s", nodeName, type.getSimpleName(),
                    node.getClass().getSimpleName()));
        }
        return (T) node;
    }

    /**
     * Constructs the path of the node with the provided {@code parent}
     * node and {@code nodeName}.
     *
     * @param parent    The parent node
     * @param childName The name of the node the path is constructed for
     * @return the constructed path of the node
     */
    public static String constructPath(YamlNode parent, String childName) {
        if (childName != null) {
            childName = NAME_APOSTROPHE_PATTERN.matcher(childName).replaceAll("\"$1\"");
        }

        if (parent != null && parent.path() != null) {
            return parent.path() + "/" + childName;
        }

        return childName;
    }

    /**
     * Checks if the provided {@code node} is a mapping
     *
     * @param node The node to check
     * @return {@code true} if the provided node is a mapping
     */
    public static boolean isMapping(YamlNode node) {
        return node instanceof YamlMapping;
    }

    /**
     * Checks if the provided {@code node} is a sequence
     *
     * @param node The node to check
     * @return {@code true} if the provided node is a sequence
     */
    public static boolean isSequence(YamlNode node) {
        return node instanceof YamlSequence;
    }

    /**
     * Checks if the provided {@code node} is a scalar
     *
     * @param node The node to check
     * @return {@code true} if the provided node is a scalar
     */
    public static boolean isScalar(YamlNode node) {
        return node instanceof YamlScalar;
    }

    /**
     * Checks if the two provided {@code nodes} are of the same type
     *
     * @param left  The left-side node of the check
     * @param right The right-side node of the check
     * @return {@code true} if the provided nodes are of the same type
     */
    public static boolean isOfSameType(YamlNode left, YamlNode right) {
        return left instanceof YamlMapping && right instanceof YamlMapping
                || left instanceof YamlSequence && right instanceof YamlSequence
                || left instanceof YamlScalar && right instanceof YamlScalar;
    }
}
