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

import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlNameNodePair;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.internal.yaml.YamlScalar;
import com.hazelcast.internal.yaml.YamlSequence;

import java.util.ArrayList;
import java.util.List;

final class YamlOrderedMappingImpl implements YamlOrderedMapping {
    private final YamlMapping wrappedMapping;
    private final List<YamlNode> randomAccessChildren;

    private YamlOrderedMappingImpl(YamlMapping wrappedMapping) {
        this.wrappedMapping = wrappedMapping;

        randomAccessChildren = new ArrayList<>(wrappedMapping.childCount());
        copyChildren();
    }

    @Override
    public YamlNode child(String name) {
        return wrappedMapping.child(name);
    }

    @Override
    public YamlMapping childAsMapping(String name) {
        return wrappedMapping.childAsMapping(name);
    }

    @Override
    public YamlSequence childAsSequence(String name) {
        return wrappedMapping.childAsSequence(name);
    }

    @Override
    public YamlScalar childAsScalar(String name) {
        return wrappedMapping.childAsScalar(name);
    }

    @Override
    public <T> T childAsScalarValue(String name) {
        return wrappedMapping.childAsScalarValue(name);
    }

    @Override
    public <T> T childAsScalarValue(String name, Class<T> type) {
        return wrappedMapping.childAsScalarValue(name, type);
    }

    @Override
    public Iterable<YamlNode> children() {
        return wrappedMapping.children();
    }

    @Override
    public Iterable<YamlNameNodePair> childrenPairs() {
        return wrappedMapping.childrenPairs();
    }

    @Override
    public int childCount() {
        return wrappedMapping.childCount();
    }

    @Override
    public YamlNode parent() {
        return wrappedMapping.parent();
    }

    @Override
    public String nodeName() {
        return wrappedMapping.nodeName();
    }

    @Override
    public String path() {
        return wrappedMapping.path();
    }

    @Override
    public YamlNode child(int index) {
        if (index >= randomAccessChildren.size()) {
            return null;
        }

        return randomAccessChildren.get(index);
    }

    static YamlOrderedMappingImpl asOrderedMapping(YamlMapping yamlMapping) {
        return new YamlOrderedMappingImpl(yamlMapping);
    }

    private void copyChildren() {
        for (YamlNode child : wrappedMapping.children()) {
            randomAccessChildren.add(child);
        }
    }
}
