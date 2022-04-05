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

import com.hazelcast.internal.yaml.YamlSequence;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import static com.hazelcast.internal.config.yaml.W3cDomUtil.asW3cNode;

class NodeListSequenceAdapter implements NodeList {
    private final YamlSequence yamlSequence;

    NodeListSequenceAdapter(YamlSequence yamlSequence) {
        this.yamlSequence = yamlSequence;
    }

    @Override
    public Node item(int index) {
        return asW3cNode(yamlSequence.child(index));
    }

    @Override
    public int getLength() {
        return yamlSequence.childCount();
    }
}
