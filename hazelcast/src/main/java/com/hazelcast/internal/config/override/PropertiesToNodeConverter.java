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

import com.hazelcast.config.InvalidConfigurationException;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

final class PropertiesToNodeConverter {

    private PropertiesToNodeConverter() {
    }

    static ConfigNode propsToNode(Map<String, String> properties) {
        String rootNode = findRootNode(properties);

        ConfigNode root = new ConfigNode(rootNode);
        for (Map.Entry<String, String> e : properties.entrySet()) {
            parseEntry(new AbstractMap.SimpleEntry<>(e.getKey().replaceFirst(rootNode + ".", ""), e.getValue()), root);
        }
        return root;
    }

    private static String findRootNode(Map<String, String> properties) {
        Set<String> rootNodeNames = properties.keySet().stream()
          .map(key -> firstNodeOf(key))
          .collect(Collectors.toSet());

        if (rootNodeNames.size() > 1) {
            throw new InvalidConfigurationException("parsed config entries have conflicting root node names");
        }

        return rootNodeNames.stream()
          .findAny()
          .orElseThrow(() -> new InvalidConfigurationException("No parsed entries found"));
    }

    private static void parseEntry(Map.Entry<String, String> entry, ConfigNode root) {
        ConfigNode last = root;
        for (String s : entry.getKey().toLowerCase().split("\\.")) {
            ConfigNode node = last.getChildren().get(s);
            if (node == null) {
                node = new ConfigNode(s, last);
                last.getChildren().put(s, node);
            }
            last = node;
        }

        last.setValue(entry.getValue());
    }

    private static String firstNodeOf(String key) {
        int dotIndex = key.indexOf(".");
        return key.substring(0, dotIndex > 0 ? dotIndex : key.length());
    }
}
