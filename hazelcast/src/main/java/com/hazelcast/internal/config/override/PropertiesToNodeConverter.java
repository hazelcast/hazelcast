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

package com.hazelcast.internal.config.override;

import com.hazelcast.config.InvalidConfigurationException;

import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A utility class converting a set of key-value pairs into a {@link ConfigNode} tree
 */
final class PropertiesToNodeConverter {

    private PropertiesToNodeConverter() {
    }

    static ConfigNode propsToNode(Map<String, String> properties) {
        String rootNode = findRootNode(properties);

        ConfigNode root = new ConfigNode(rootNode);
        for (Map.Entry<String, String> e : properties.entrySet()) {
            parseEntry(e.getKey().replaceFirst(rootNode + ".", ""), e.getValue(), root);
        }
        return root;
    }

    private static String findRootNode(Map<String, String> properties) {
        Set<String> rootNodeNames = properties.keySet().stream()
          .map(PropertiesToNodeConverter::firstNodeOf)
          .collect(Collectors.toSet());

        if (rootNodeNames.size() > 1) {
            throw new InvalidConfigurationException("could not determine a root config node name, multiple found: "
              + rootNodeNames.stream().collect(Collectors.joining(", ", "[", "]")));
        }

        return rootNodeNames.stream()
          .findAny()
          .orElseThrow(() -> new InvalidConfigurationException("No parsed entries found"));
    }

    private static void parseEntry(String key, String value, ConfigNode root) {
        ConfigNode last = root;
        for (String s : lowerCaseInternal(key).split("\\.")) {
            ConfigNode node = last.getChildren().get(s);
            if (node == null) {
                node = new ConfigNode(s, last);
                last.getChildren().put(s, node);
            }
            last = node;
        }

        last.setValue(value);
    }

    private static String firstNodeOf(String key) {
        int dotIndex = key.indexOf(".");
        return key.substring(0, dotIndex > 0 ? dotIndex : key.length());
    }
}
