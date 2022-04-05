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

import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.TypeInfo;
import org.w3c.dom.UserDataHandler;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A class adapting {@link ConfigNode}s to {@link Element}.
 * <p>
 * Used for processing external configuration overrides.
 */
@SuppressWarnings({"checkstyle:methodcount"})
class ConfigOverrideElementAdapter implements Element {
    private final ConfigNode configNode;

    ConfigOverrideElementAdapter(@Nonnull ConfigNode node) {
        Objects.requireNonNull(node);
        this.configNode = node;
    }

    @Override
    public String getNodeName() {
        return configNode.getName();
    }

    @Override
    public String getNodeValue() throws DOMException {
        return configNode.getValue();
    }

    @Override
    public short getNodeType() {
        return Node.ELEMENT_NODE;
    }

    @Override
    public Node getParentNode() {
        return configNode.getParent().map(ConfigOverrideElementAdapter::new).orElse(null);
    }

    @Override
    public NodeList getChildNodes() {
        return new NodeList() {
            private final List<Node> children = configNode.getChildren().values().stream()
              .map(ConfigOverrideElementAdapter::new)
              .collect(Collectors.toList());

            @Override
            public Node item(int index) {
                return children.get(index);
            }

            @Override
            public int getLength() {
                return children.size();
            }
        };
    }

    @Override
    public void setNodeValue(String nodeValue) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node getFirstChild() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node getLastChild() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node getPreviousSibling() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node getNextSibling() {
        throw new UnsupportedOperationException();
    }

    @Override
    public NamedNodeMap getAttributes() {
        return new NamedNodeAdapter();
    }

    @Override
    public Document getOwnerDocument() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node insertBefore(Node newChild, Node refChild) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node replaceChild(Node newChild, Node oldChild) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node removeChild(Node oldChild) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node appendChild(Node newChild) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasChildNodes() {
        return !configNode.getChildren().isEmpty();
    }

    @Override
    public Node cloneNode(boolean deep) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void normalize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSupported(String feature, String version) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getNamespaceURI() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getPrefix() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPrefix(String prefix) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getLocalName() {
        return getNodeName();
    }

    @Override
    public boolean hasAttributes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getBaseURI() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short compareDocumentPosition(Node other) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTextContent() throws DOMException {
        return getNodeValue();
    }

    @Override
    public void setTextContent(String textContent) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSameNode(Node other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String lookupPrefix(String namespaceURI) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDefaultNamespace(String namespaceURI) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String lookupNamespaceURI(String prefix) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEqualNode(Node arg) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getFeature(String feature, String version) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object setUserData(String key, Object data, UserDataHandler handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getUserData(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTagName() {
        return getNodeName();
    }

    @Override
    public String getAttribute(String name) {
        ConfigNode configNode = this.configNode.getChildren().get(name);
        return configNode != null && configNode.hasValue()
          ? configNode.getValue()
          : "";
    }

    @Override
    public void setAttribute(String name, String value) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAttribute(String name) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Attr getAttributeNode(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Attr setAttributeNode(Attr newAttr) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Attr removeAttributeNode(Attr oldAttr) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NodeList getElementsByTagName(String name) {
        return new SingletonNodeList(getAttributes().getNamedItem(name));
    }

    @Override
    public String getAttributeNS(String namespaceURI, String localName) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAttributeNS(String namespaceURI, String qualifiedName, String value) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAttributeNS(String namespaceURI, String localName) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Attr getAttributeNodeNS(String namespaceURI, String localName) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Attr setAttributeNodeNS(Attr newAttr) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NodeList getElementsByTagNameNS(String namespaceURI, String localName) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasAttribute(String name) {
        return getAttributes().getNamedItem(name) != null;
    }

    @Override
    public boolean hasAttributeNS(String namespaceURI, String localName) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TypeInfo getSchemaTypeInfo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setIdAttribute(String name, boolean isId) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setIdAttributeNS(String namespaceURI, String localName, boolean isId) throws DOMException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setIdAttributeNode(Attr idAttr, boolean isId) throws DOMException {
        throw new UnsupportedOperationException();
    }

    static class SingletonNodeList implements NodeList {

        private final Node element;

        SingletonNodeList(Node element) {
            this.element = element;
        }

        @Override
        public Node item(int index) {
            return index != 0 ? null : element;
        }

        @Override
        public int getLength() {
            return 0;
        }
    }

    private class NamedNodeAdapter implements NamedNodeMap {

        @Override
        public Node getNamedItem(String name) {
            return configNode.getChildren().get(name) == null
              ? null
              : new ConfigOverrideElementAdapter(configNode.getChildren().get(name));
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
            return new ConfigOverrideElementAdapter(new ArrayList<>(configNode.getChildren().values()).get(index));
        }

        @Override
        public int getLength() {
            return configNode.getChildren().size();
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
}
