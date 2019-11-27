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

package com.hazelcast.internal.config.processors;

import com.hazelcast.config.MerkleTreeConfig;

import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.internal.config.DomConfigHelper.getIntegerValue;
import static com.hazelcast.internal.config.DomConfigHelper.getTextContent;
import static com.hazelcast.internal.config.DomConfigHelper.streamOfAttributes;
import static com.hazelcast.internal.config.DomConfigHelper.streamOfChildElements;

public class MerkleTreeConfigProcessor implements Processor<MerkleTreeConfig> {
    private final Node node;
    private final Map<String, BiConsumer<MerkleTreeConfig, Node>> map = new HashMap<>();

    MerkleTreeConfigProcessor(Node node, boolean domLevel3) {
        this.node = node;
        map.put(
            "enabled",
            (merkleTreeConfig, child) -> merkleTreeConfig.setEnabled(getBooleanValue(getTextContent(child, domLevel3))));
        map.put(
            "depth",
            (merkleTreeConfig, child) -> merkleTreeConfig.setDepth(getIntegerValue("depth", getTextContent(child, domLevel3))));
    }

    @Override
    public MerkleTreeConfig process() {
        MerkleTreeConfig merkleTreeConfig = new MerkleTreeConfig();
        Stream.concat(streamOfAttributes(node), streamOfChildElements(node))
            .forEach(node -> map.get(cleanNodeName(node)).accept(merkleTreeConfig, node));
        return merkleTreeConfig;
    }
}
