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
import com.hazelcast.internal.yaml.YamlNode;
import org.w3c.dom.DOMException;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 * Empty {@link NamedNodeMap} implementation. Used by {@link YamlElementAdapter#getAttributes()}
 * when wrapping {@link YamlNode}s other than {@link YamlMapping}.
 */
final class EmptyNamedNodeMap implements NamedNodeMap {
    private static final NamedNodeMap INSTANCE = new EmptyNamedNodeMap();

    private EmptyNamedNodeMap() {
    }

    @Override
    public Node getNamedItem(String name) {
        return null;
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
        return null;
    }

    @Override
    public int getLength() {
        return 0;
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

    /**
     * Returns the singleton instance of this class
     *
     * @return an empty {@link NamedNodeMap}
     * @see #INSTANCE
     */
    static NamedNodeMap emptyNamedNodeMap() {
        return INSTANCE;
    }
}
