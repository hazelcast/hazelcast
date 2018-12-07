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

package com.hazelcast.config.yaml;

import com.hazelcast.internal.yaml.YamlCollection;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlNode;
import com.hazelcast.internal.yaml.YamlScalar;
import com.hazelcast.internal.yaml.YamlSequence;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.UserDataHandler;

import static com.hazelcast.config.yaml.EmptyNamedNodeMap.emptyNamedNodeMap;
import static com.hazelcast.config.yaml.EmptyNodeList.emptyNodeList;

class NodeAdapter implements Node {
    private final YamlNode yamlNode;

    NodeAdapter(YamlNode yamlNode) {
        this.yamlNode = yamlNode;
    }

    @Override
    public String getNodeName() {
        return yamlNode.nodeName();
    }

    @Override
    public String getNodeValue() throws DOMException {
        if (yamlNode instanceof YamlScalar) {
            return ((YamlScalar) yamlNode).nodeValue().toString();
        }
        return null;
    }

    @Override
    public void setNodeValue(String nodeValue) throws DOMException {
        throw new UnsupportedOperationException();
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
        if (!(yamlNode instanceof YamlCollection) || ((YamlCollection) yamlNode).childCount() == 0) {
            return emptyNodeList();
        }

        if (yamlNode instanceof YamlMapping) {
            return new NodeListMappingAdapter((YamlMapping) yamlNode);
        }

        return new NodeListSequenceAdapter((YamlSequence) yamlNode);
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
        throw new UnsupportedOperationException();
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
}
