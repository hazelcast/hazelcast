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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class ConfigNodeStateTracker {
    Map<String, String> unprocessedNodes(ConfigNode node) {
        List<ConfigNode> nodes = new ArrayList<>();

        findAllUnreadNodes(nodes, node);

        return nodes.stream().map(this::process).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private void findAllUnreadNodes(List<ConfigNode> acc, ConfigNode node) {
        if (node.hasValue() && !node.isRead()) {
            acc.add(node);
        }

        node.getChildren().values().forEach(c -> findAllUnreadNodes(acc, c));
    }

    private Map.Entry<String, String> process(ConfigNode config) {
        List<String> configSegments = new ArrayList<>();
        ConfigNode current = config;
        do {
            configSegments.add(current.getName());
            current = current.getParent().orElse(null);
        }
        while (current != null);

        Collections.reverse(configSegments);

        return new AbstractMap.SimpleEntry<>(String.join(".", configSegments), config.getValue());
    }
}
