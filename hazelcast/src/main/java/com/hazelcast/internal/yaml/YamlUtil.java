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

package com.hazelcast.internal.yaml;

import com.hazelcast.internal.util.JavaVersion;

/**
 * Utility class for working with YAML nodes.
 */
public final class YamlUtil {
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
     * Checks if the runtime environment is Java8 or higher. If the
     * runtime is an older version the check throws
     * {@link UnsupportedOperationException}.
     *
     * @throws UnsupportedOperationException If the runtime environment
     *                                       is not Java8 or higher
     */
    public static void ensureRunningOnJava8OrHigher() {
        if (!JavaVersion.isAtLeast(JavaVersion.JAVA_1_8)) {
            throw new UnsupportedOperationException("Processing YAML documents requires Java 8 or higher version");
        }
    }
}
