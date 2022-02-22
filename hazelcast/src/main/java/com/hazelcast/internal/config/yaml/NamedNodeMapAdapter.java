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
import org.w3c.dom.DOMException;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import static com.hazelcast.internal.config.yaml.W3cDomUtil.asW3cNode;
import static com.hazelcast.internal.config.yaml.YamlOrderedMappingImpl.asOrderedMapping;

/**
 * Class adapting {@link YamlMapping} to {@link NamedNodeMap}
 *
 * @see YamlOrderedMapping
 */
class NamedNodeMapAdapter implements NamedNodeMap {
    private final YamlOrderedMapping yamlMapping;

    NamedNodeMapAdapter(YamlMapping yamlMapping) {
        this.yamlMapping = asOrderedMapping(yamlMapping);
    }

    @Override
    public Node getNamedItem(String name) {
        return asW3cNode(yamlMapping.child(name));
    }

    @Override
    public Node setNamedItem(Node arg) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node removeNamedItem(String name) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node item(int index) {
        return asW3cNode(yamlMapping.child(index));
    }

    @Override
    public int getLength() {
        return yamlMapping.childCount();
    }

    @Override
    public Node getNamedItemNS(String namespaceURI, String localName) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node setNamedItemNS(Node arg) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node removeNamedItemNS(String namespaceURI, String localName) throws DOMException {
        throw new UnsupportedOperationException();
    }
}
