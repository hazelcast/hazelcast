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

package com.hazelcast.config;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.StringUtil;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.String.format;

/**
 * Helper class for accessing and extracting values from W3C DOM {@link Node}
 * instances
 *
 * @see AbstractDomConfigProcessor
 * @see AbstractXmlConfigHelper
 */
public final class DomConfigHelper {
    private DomConfigHelper() {
    }

    public static void fillProperties(final Node node, Map<String, Comparable> properties, boolean domLevel3) {
        if (properties == null) {
            return;
        }
        for (Node n : childElements(node)) {
            if (n.getNodeType() == Node.TEXT_NODE || n.getNodeType() == Node.COMMENT_NODE) {
                continue;
            }
            final String name = cleanNodeName(n);
            final String propertyName;
            if ("property".equals(name)) {
                propertyName = getTextContent(n.getAttributes().getNamedItem("name"), domLevel3).trim();
            } else {
                // old way - probably should be deprecated
                propertyName = name;
            }
            final String value = getTextContent(n, domLevel3).trim();
            properties.put(propertyName, value);
        }
    }

    public static void fillProperties(final Node node, Properties properties, boolean domLevel3) {
        if (properties == null) {
            return;
        }
        for (Node n : childElements(node)) {
            final String name = cleanNodeName(n);
            final String propertyName = "property".equals(name)
                    ? getTextContent(n.getAttributes().getNamedItem("name"), domLevel3).trim()
                    // old way - probably should be deprecated
                    : name;
            final String value = getTextContent(n, domLevel3).trim();
            properties.setProperty(propertyName, value);
        }
    }

    public static Iterable<Node> childElements(Node node) {
        return new IterableNodeList(node, Node.ELEMENT_NODE);
    }

    public static Iterable<Node> asElementIterable(NodeList list) {
        return new IterableNodeList(list, Node.ELEMENT_NODE);
    }

    public static String cleanNodeName(final Node node) {
        final String nodeName = node.getLocalName();
        if (nodeName == null) {
            throw new HazelcastException("Local node name is null for " + node);
        }
        return StringUtil.lowerCaseInternal(nodeName);
    }

    public static String getTextContent(final Node node, boolean domLevel3) {
        if (node != null) {
            final String text;
            if (domLevel3) {
                text = node.getTextContent();
            } else {
                text = getTextContentOld(node);
            }
            return text != null ? text.trim() : "";
        }
        return "";
    }

    private static String getTextContentOld(final Node node) {
        final Node child = node.getFirstChild();
        if (child != null) {
            final Node next = child.getNextSibling();
            if (next == null) {
                return hasTextContent(child) ? child.getNodeValue() : "";
            }
            final StringBuilder buf = new StringBuilder();
            appendTextContents(node, buf);
            return buf.toString();
        }
        return "";
    }

    private static void appendTextContents(final Node node, final StringBuilder buf) {
        Node child = node.getFirstChild();
        while (child != null) {
            if (hasTextContent(child)) {
                buf.append(child.getNodeValue());
            }
            child = child.getNextSibling();
        }
    }

    private static boolean hasTextContent(final Node node) {
        final short nodeType = node.getNodeType();
        return nodeType != Node.COMMENT_NODE && nodeType != Node.PROCESSING_INSTRUCTION_NODE;
    }

    public static boolean getBooleanValue(final String value) {
        return parseBoolean(StringUtil.lowerCaseInternal(value));
    }

    public static int getIntegerValue(final String parameterName, final String value) {
        try {
            return Integer.parseInt(value);
        } catch (final NumberFormatException e) {
            throw new InvalidConfigurationException(format("Invalid integer value for parameter %s: %s", parameterName, value));
        }
    }

    public static int getIntegerValue(final String parameterName, final String value, int defaultValue) {
        if (isNullOrEmpty(value)) {
            return defaultValue;
        }
        return getIntegerValue(parameterName, value);
    }

    public static long getLongValue(final String parameterName, final String value) {
        try {
            return Long.parseLong(value);
        } catch (final Exception e) {
            throw new InvalidConfigurationException(
                    format("Invalid long integer value for parameter %s: %s", parameterName, value));
        }
    }

    public static long getLongValue(final String parameterName, final String value, long defaultValue) {
        if (isNullOrEmpty(value)) {
            return defaultValue;
        }
        return getLongValue(parameterName, value);
    }

    public static double getDoubleValue(final String parameterName, final String value) {
        try {
            return parseDouble(value);
        } catch (final Exception e) {
            throw new InvalidConfigurationException(
                    format("Invalid long integer value for parameter %s: %s", parameterName, value));
        }
    }

    public static double getDoubleValue(final String parameterName, final String value, double defaultValue) {
        if (isNullOrEmpty(value)) {
            return defaultValue;
        }
        return getDoubleValue(parameterName, value);
    }

    public static String getAttribute(Node node, String attName, boolean domLevel3) {
        final Node attNode = node.getAttributes().getNamedItem(attName);
        if (attNode == null) {
            return null;
        }
        return getTextContent(attNode, domLevel3);
    }

    private static class IterableNodeList implements Iterable<Node> {

        private final NodeList wrapped;
        private final int maximum;
        private final short nodeType;

        IterableNodeList(Node parent, short nodeType) {
            this(parent.getChildNodes(), nodeType);
        }

        IterableNodeList(NodeList wrapped, short nodeType) {
            this.wrapped = wrapped;
            this.nodeType = nodeType;
            this.maximum = wrapped.getLength();
        }

        @Override
        public Iterator<Node> iterator() {
            return new Iterator<Node>() {
                private int index;
                private Node next;

                public boolean hasNext() {
                    next = null;
                    for (; index < maximum; index++) {
                        final Node item = wrapped.item(index);
                        if (nodeType == 0 || item.getNodeType() == nodeType) {
                            next = item;
                            return true;
                        }
                    }
                    return false;
                }

                public Node next() {
                    if (hasNext()) {
                        index++;
                        return next;
                    }
                    throw new NoSuchElementException();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

}
