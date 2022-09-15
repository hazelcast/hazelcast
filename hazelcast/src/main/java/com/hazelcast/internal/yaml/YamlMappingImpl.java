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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.hazelcast.internal.yaml.YamlUtil.asMapping;
import static com.hazelcast.internal.yaml.YamlUtil.asScalar;
import static com.hazelcast.internal.yaml.YamlUtil.asSequence;

public class YamlMappingImpl extends AbstractYamlNode implements MutableYamlMapping {
    private Map<String, YamlNode> children = Collections.emptyMap();

    YamlMappingImpl(YamlNode parent, String nodeName) {
        super(parent, nodeName);
    }

    @Override
    public YamlNode child(String name) {
        return children.get(name);
    }

    @Override
    public YamlMapping childAsMapping(String name) {
        return asMapping(child(name));
    }

    @Override
    public YamlSequence childAsSequence(String name) {
        return asSequence(child(name));
    }

    @Override
    public YamlScalar childAsScalar(String name) {
        return asScalar(child(name));
    }

    @Override
    public <T> T childAsScalarValue(String name) {
        return childAsScalar(name).nodeValue();
    }

    @Override
    public <T> T childAsScalarValue(String name, Class<T> type) {
        return childAsScalar(name).nodeValue(type);
    }

    @Override
    public Iterable<YamlNode> children() {
        return children.values().stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    public Iterable<YamlNameNodePair> childrenPairs() {
        List<YamlNameNodePair> pairs = new LinkedList<>();
        for (Map.Entry<String, YamlNode> child : children.entrySet()) {
            pairs.add(new YamlNameNodePair(child.getKey(), child.getValue()));
        }
        return pairs;
    }

    @Override
    public void addChild(String name, YamlNode node) {
        getOrCreateChildren().put(name, node);
    }

    @Override
    public void removeChild(String name) {
        children.remove(name);
    }

    private Map<String, YamlNode> getOrCreateChildren() {
        if (children == Collections.<String, YamlNode>emptyMap()) {
            children = new LinkedHashMap<>();
        }

        return children;
    }

    @Override
    public int childCount() {
        return children.size();
    }

    @Override
    public String toString() {
        return "YamlMappingImpl{"
                + "nodeName=" + nodeName()
                + ", children=" + children
                + '}';
    }

}
