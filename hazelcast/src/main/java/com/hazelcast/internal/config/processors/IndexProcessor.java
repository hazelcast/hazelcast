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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.internal.config.DomConfigHelper;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getTextContent;
import static com.hazelcast.internal.config.DomConfigHelper.streamOfChildElements;

class IndexProcessor implements Processor<IndexConfig> {
    private final Node node;
    private final boolean domLevel3;

    IndexProcessor(Node node, boolean domLevel3) {
        this.node = node;
        this.domLevel3 = domLevel3;
    }

    @Override
    public IndexConfig process() {
        IndexConfig indexConfig = new IndexConfig().setName(name()).setType(type());

        streamOfChildElements(node)
            .filter(attributesNode -> "attributes".equals(cleanNodeName(attributesNode)))
            .flatMap(DomConfigHelper::streamOfChildElements)
            .filter(attributeNode -> "attribute".equals(cleanNodeName(attributeNode)))
            .map(attributeNode -> getTextContent(attributeNode, domLevel3))
            .forEach(indexConfig::addAttribute);
        return indexConfig;
    }

    private IndexType type() {
        NamedNodeMap attrs = node.getAttributes();
        String typeStr = getTextContent(attrs.getNamedItem("type"), domLevel3);
        if (typeStr.isEmpty()) {
            typeStr = IndexConfig.DEFAULT_TYPE.name();
        }
        typeStr = typeStr.toLowerCase();
        IndexType type;
        if (typeStr.equals(IndexType.SORTED.name().toLowerCase())) {
            type = IndexType.SORTED;
        } else if (typeStr.equals(IndexType.HASH.name().toLowerCase())) {
            type = IndexType.HASH;
        } else {
            throw new IllegalArgumentException("Unsupported index type: " + typeStr);
        }
        return type;
    }

    private String name() {
        NamedNodeMap attrs = node.getAttributes();
        String name = getTextContent(attrs.getNamedItem("name"), domLevel3);
        if (name.isEmpty()) {
            name = null;
        }
        return name;
    }
}
