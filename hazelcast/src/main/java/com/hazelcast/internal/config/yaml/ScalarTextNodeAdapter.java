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
import com.hazelcast.internal.yaml.YamlScalar;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.UserDataHandler;

import static com.hazelcast.internal.config.yaml.EmptyNodeList.emptyNodeList;

@SuppressWarnings({"checkstyle:methodcount"})
public class ScalarTextNodeAdapter implements Node {
    private YamlScalar scalar;

    ScalarTextNodeAdapter(YamlScalar scalar) {
        this.scalar = scalar;
    }

    @Override
    public String getNodeName() {
        return scalar.nodeName();
    }

    @Override
    public String getNodeValue() throws DOMException {
        Object nodeValue = scalar.nodeValue();
        return nodeValue != null ? nodeValue.toString() : null;
    }

    public Object getNodeRawValue() {
        return scalar.nodeValue();
    }

    @Override
    public void setNodeValue(String nodeValue) throws DOMException {
        if (scalar instanceof MutableYamlScalar) {
            ((MutableYamlScalar) scalar).setValue(nodeValue);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public short getNodeType() {
        return Node.TEXT_NODE;
    }

    @Override
    public Node getParentNode() {
        return W3cDomUtil.asW3cNode(scalar.parent());
    }

    @Override
    public NodeList getChildNodes() {
        return emptyNodeList();
    }

    @Override
    public Node getFirstChild() {
        return null;
    }

    @Override
    public Node getLastChild() {
        return null;
    }

    @Override
    public Node getPreviousSibling() {
        return null;
    }

    @Override
    public Node getNextSibling() {
        return null;
    }

    @Override
    public NamedNodeMap getAttributes() {
        return null;
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
        return false;
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
        return false;
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
        return false;
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
