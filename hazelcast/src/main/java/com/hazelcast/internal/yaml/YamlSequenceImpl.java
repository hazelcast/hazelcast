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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.internal.yaml.YamlUtil.asMapping;
import static com.hazelcast.internal.yaml.YamlUtil.asScalar;
import static com.hazelcast.internal.yaml.YamlUtil.asSequence;

class YamlSequenceImpl extends AbstractYamlNode implements MutableYamlSequence {
    private List<YamlNode> children = Collections.emptyList();

    YamlSequenceImpl(YamlNode parent, String nodeName) {
        super(parent, nodeName);
    }

    @Override
    public YamlNode child(int index) {
        if (index >= children.size()) {
            return null;
        }
        return children.get(index);
    }

    @Override
    public Iterable<YamlNode> children() {
        return children;
    }

    @Override
    public YamlMapping childAsMapping(int index) {
        return asMapping(child(index));
    }

    @Override
    public YamlSequence childAsSequence(int index) {
        return asSequence(child(index));
    }

    @Override
    public YamlScalar childAsScalar(int index) {
        return asScalar(child(index));
    }

    @Override
    public <T> T childAsScalarValue(int index) {
        return childAsScalar(index).nodeValue();
    }

    @Override
    public <T> T childAsScalarValue(int index, Class<T> type) {
        return childAsScalar(index).nodeValue(type);
    }

    @Override
    public void addChild(YamlNode child) {
        getOrCreateChildren().add(child);
    }

    private List<YamlNode> getOrCreateChildren() {
        if (children == Collections.<YamlNode>emptyList()) {
            children = new ArrayList<YamlNode>();
        }

        return children;
    }

    @Override
    public int childCount() {
        return children.size();
    }

    @Override
    public String toString() {
        return "YamlSequenceImpl{"
                + "nodeName=" + nodeName()
                + ", children=" + children
                + '}';
    }
}
