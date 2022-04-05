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

import com.hazelcast.internal.yaml.MutableYamlScalar;
import com.hazelcast.internal.yaml.YamlCollection;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.internal.yaml.YamlScalar;
import com.hazelcast.internal.yaml.YamlSequence;
import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.TypeInfo;
import org.w3c.dom.UserDataHandler;

import static com.hazelcast.internal.config.yaml.EmptyNamedNodeMap.emptyNamedNodeMap;
import static com.hazelcast.internal.config.yaml.EmptyNodeList.emptyNodeList;

/**
 * Class adapting {@link YamlNode}s to {@link Element}.
 * <p>
 * Used for processing YAML configuration.
 */
@SuppressWarnings({"checkstyle:methodcount"})
public class YamlElementAdapter implements Element {
    private final YamlNode yamlNode;

    YamlElementAdapter(YamlNode yamlNode) {
        this.yamlNode = yamlNode;
    }

    public YamlNode getYamlNode() {
        return yamlNode;
    }

    @Override
    public String getNodeName() {
        return yamlNode.nodeName();
    }

    @Override
    public String getNodeValue() throws DOMException {
        if (yamlNode instanceof YamlScalar) {
            Object nodeValue = ((YamlScalar) yamlNode).nodeValue();
            return nodeValue != null ? nodeValue.toString() : null;
        }
        return null;
    }

    @Override
    public void setNodeValue(String nodeValue) throws DOMException {
        if (yamlNode instanceof MutableYamlScalar) {
            ((MutableYamlScalar) yamlNode).setValue(nodeValue);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public short getNodeType() {
        return Node.ELEMENT_NODE;
    }

    @Override
    public Node getParentNode() {
        return W3cDomUtil.asW3cNode(yamlNode.parent());
    }

    @Override
    public NodeList getChildNodes() {
        if (!hasChildNodes()) {
            return emptyNodeList();
        }

        if (yamlNode instanceof YamlMapping) {
            return new NodeListMappingAdapter((YamlMapping) yamlNode);
        } else if (yamlNode instanceof YamlSequence) {
            return new NodeListSequenceAdapter((YamlSequence) yamlNode);
        }

        return new NodeListScalarAdapter((YamlScalar) yamlNode);
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
        if (yamlNode instanceof YamlMapping) {
            return new NamedNodeMapAdapter((YamlMapping) yamlNode);
        }
        return emptyNamedNodeMap();
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
        return yamlNode instanceof YamlCollection && ((YamlCollection) yamlNode).childCount() > 0
                || yamlNode instanceof YamlScalar;
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
        if (yamlNode instanceof YamlMapping) {
            YamlScalar yamlScalar = ((YamlMapping) yamlNode).childAsScalar(name);
            if (yamlScalar != null) {
                return yamlScalar.nodeValue().toString();
            }
        }
        return "";
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
        // since this class adapts YAML documents that don't allow
        // duplicates, there could be only one element with the given name
        Node element = getAttributes().getNamedItem(name);

        return W3cDomUtil.asNodeList(element);
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
}
