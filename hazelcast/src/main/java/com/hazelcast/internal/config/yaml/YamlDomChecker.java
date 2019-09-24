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

package com.hazelcast.internal.config.yaml;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlNameNodePair;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.internal.yaml.YamlScalar;
import com.hazelcast.internal.yaml.YamlSequence;
import com.hazelcast.internal.yaml.YamlUtil;

/**
 * Utility class for checking the provided YAML DOM for {@code null}
 * scalar values and mappings or sequences with {@code null} child nodes.
 */
public final class YamlDomChecker {

    private YamlDomChecker() {
    }

    /**
     * Performs {code @null} checks on the provided YAML node recursively.
     *
     * @param node The YAML node to check for {@code null}s
     */
    public static void check(YamlNode node) {
        if (node instanceof YamlMapping) {
            for (YamlNameNodePair nodePair : ((YamlMapping) node).childrenPairs()) {
                YamlNode child = nodePair.childNode();
                if (child == null) {
                    String path = YamlUtil.constructPath(node, nodePair.nodeName());
                    reportNullEntryOnConcretePath(path);
                }

                check(nodePair.childNode());

            }
        } else if (node instanceof YamlSequence) {
            for (YamlNode child : ((YamlSequence) node).children()) {
                if (child == null) {
                    throw new InvalidConfigurationException("There is a null configuration entry under sequence " + node.path()
                            + ". Please check if the provided YAML configuration is well-indented and no blocks started without "
                            + "sub-nodes.");
                }

                check(child);
            }
        } else {
            if (((YamlScalar) node).nodeValue() == null) {
                reportNullEntryOnConcretePath(node.path());
            }
        }
    }

    private static void reportNullEntryOnConcretePath(String path) {
        throw new InvalidConfigurationException("The configuration entry under " + path
                + " is null. Please check if the provided YAML configuration is well-indented and no blocks started"
                + " without sub-nodes.");
    }
}
