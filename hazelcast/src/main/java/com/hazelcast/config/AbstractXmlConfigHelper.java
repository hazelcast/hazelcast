/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.nio.ByteOrder;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;

import static com.hazelcast.util.StringUtil.upperCaseInternal;

/**
 * Contains Hazelcast Xml Configuration helper methods and variables.
 */
public abstract class AbstractXmlConfigHelper {

    private static final ILogger LOGGER = Logger.getLogger(AbstractXmlConfigHelper.class);

    protected boolean domLevel3 = true;

    /**
     * Iterator for NodeList
     */
    public static class IterableNodeList implements Iterable<Node> {

        private final NodeList parent;
        private final int maximum;
        private final short nodeType;

        public IterableNodeList(final Node node) {
            this(node.getChildNodes());
        }

        public IterableNodeList(final NodeList list) {
            this(list, (short) 0);
        }

        public IterableNodeList(final Node node, short nodeType) {
            this(node.getChildNodes(), nodeType);
        }

        public IterableNodeList(final NodeList parent, short nodeType) {
            this.parent = parent;
            this.nodeType = nodeType;
            this.maximum = parent.getLength();
        }

        public Iterator<Node> iterator() {
            return new Iterator<Node>() {
                private int index;
                private Node next;
                private boolean findNext() {
                    next = null;
                    for (; index < maximum; index++) {
                        final Node item = parent.item(index);
                        if (nodeType == 0 || item.getNodeType() == nodeType) {
                            next = item;
                            return true;
                        }
                    }
                    return false;
                }

                public boolean hasNext() {
                    return findNext();
                }
                public Node next() {
                    if (findNext()) {
                        index++;
                        return next;
                    }
                    throw new NoSuchElementException();
                }
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    protected String xmlToJavaName(final String name) {
        final StringBuilder builder = new StringBuilder();
        final char[] charArray = name.toCharArray();
        boolean dash = false;
        final StringBuilder token = new StringBuilder();
        for (char aCharArray : charArray) {
            if (aCharArray == '-') {
                appendToken(builder, token);
                dash = true;
                continue;
            }
            token.append(dash ? Character.toUpperCase(aCharArray) : aCharArray);
            dash = false;
        }
        appendToken(builder, token);
        return builder.toString();
    }

    protected void appendToken(final StringBuilder builder, final StringBuilder token) {
        String string = token.toString();
        if ("Jvm".equals(string)) {
            string = "JVM";
        }
        builder.append(string);
        token.setLength(0);
    }

    protected String getTextContent(final Node node) {
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

    private String getTextContentOld(final Node node) {
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

    private void appendTextContents(final Node node, final StringBuilder buf) {
        Node child = node.getFirstChild();
        while (child != null) {
            if (hasTextContent(child)) {
                buf.append(child.getNodeValue());
            }
            child = child.getNextSibling();
        }
    }

    protected final boolean hasTextContent(final Node node) {
        final short nodeType = node.getNodeType();
        return nodeType != Node.COMMENT_NODE && nodeType != Node.PROCESSING_INSTRUCTION_NODE;
    }

    public final String cleanNodeName(final Node node) {
        return cleanNodeName(node.getNodeName());
    }

    public static String cleanNodeName(final String nodeName) {
        String name = nodeName;
        if (name != null) {
            name = nodeName.replaceAll("\\w+:", "").toLowerCase();
        }
        return name;
    }

    protected boolean checkTrue(final String value) {
        return "true".equalsIgnoreCase(value)
                || "yes".equalsIgnoreCase(value)
                || "on".equalsIgnoreCase(value);
    }

    protected int getIntegerValue(final String parameterName, final String value, final int defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (final Exception e) {
            LOGGER.info(parameterName + " parameter value, [" + value
                    + "], is not a proper integer. Default value, [" + defaultValue + "], will be used!");
            LOGGER.warning(e);
            return defaultValue;
        }
    }

    protected long getLongValue(final String parameterName, final String value, final long defaultValue) {
        try {
            return Long.parseLong(value);
        } catch (final Exception e) {
            LOGGER.info(parameterName + " parameter value, [" + value
                    + "], is not a proper long. Default value, [" + defaultValue + "], will be used!");
            LOGGER.warning(e);
            return defaultValue;
        }
    }

    protected String getAttribute(org.w3c.dom.Node node, String attName) {
        final Node attNode = node.getAttributes().getNamedItem(attName);
        if (attNode == null) {
            return null;
        }
        return getTextContent(attNode);
    }

    protected SocketInterceptorConfig parseSocketInterceptorConfig(final org.w3c.dom.Node node) {
        SocketInterceptorConfig socketInterceptorConfig = new SocketInterceptorConfig();
        final NamedNodeMap atts = node.getAttributes();
        final Node enabledNode = atts.getNamedItem("enabled");
        final boolean enabled = enabledNode != null && checkTrue(getTextContent(enabledNode).trim());
        socketInterceptorConfig.setEnabled(enabled);

        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            if ("class-name".equals(nodeName)) {
                socketInterceptorConfig.setClassName(getTextContent(n).trim());
            } else if ("properties".equals(nodeName)) {
                fillProperties(n, socketInterceptorConfig.getProperties());
            }
        }
        return socketInterceptorConfig;
    }

    protected void fillProperties(final org.w3c.dom.Node node, Properties properties) {
        if (properties == null) {
            return;
        }
        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            if (n.getNodeType() == org.w3c.dom.Node.TEXT_NODE || n.getNodeType() == org.w3c.dom.Node.COMMENT_NODE) {
                continue;
            }
            final String name = cleanNodeName(n.getNodeName());
            final String propertyName;
            if ("property".equals(name)) {
                propertyName = getTextContent(n.getAttributes().getNamedItem("name")).trim();
            } else {
                // old way - probably should be deprecated
                propertyName = name;
            }
            final String value = getTextContent(n).trim();
            properties.setProperty(propertyName, value);
        }
    }


    protected SerializationConfig parseSerialization(final Node node) {
        SerializationConfig serializationConfig = new SerializationConfig();
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String name = cleanNodeName(child);
            if ("portable-version".equals(name)) {
                String value = getTextContent(child);
                serializationConfig.setPortableVersion(getIntegerValue(name, value, 0));
            } else if ("check-class-def-errors".equals(name)) {
                String value = getTextContent(child);
                serializationConfig.setCheckClassDefErrors(checkTrue(value));
            } else if ("use-native-byte-order".equals(name)) {
                serializationConfig.setUseNativeByteOrder(checkTrue(getTextContent(child)));
            } else if ("byte-order".equals(name)) {
                String value = getTextContent(child);
                ByteOrder byteOrder = null;
                if (ByteOrder.BIG_ENDIAN.toString().equals(value)) {
                    byteOrder = ByteOrder.BIG_ENDIAN;
                } else if (ByteOrder.LITTLE_ENDIAN.toString().equals(value)) {
                    byteOrder = ByteOrder.LITTLE_ENDIAN;
                }
                serializationConfig.setByteOrder(byteOrder != null ? byteOrder : ByteOrder.BIG_ENDIAN);
            } else if ("enable-compression".equals(name)) {
                serializationConfig.setEnableCompression(checkTrue(getTextContent(child)));
            } else if ("enable-shared-object".equals(name)) {
                serializationConfig.setEnableSharedObject(checkTrue(getTextContent(child)));
            } else if ("allow-unsafe".equals(name)) {
                serializationConfig.setAllowUnsafe(checkTrue(getTextContent(child)));
            } else if ("data-serializable-factories".equals(name)) {
                fillDataSerializableFactories(child, serializationConfig);
            } else if ("portable-factories".equals(name)) {
                fillPortableFactories(child, serializationConfig);
            } else if ("serializers".equals(name)) {
                fillSerializers(child, serializationConfig);
            }
        }
        return serializationConfig;
    }

    protected void fillDataSerializableFactories(Node node, SerializationConfig serializationConfig) {
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String name = cleanNodeName(child);
            if ("data-serializable-factory".equals(name)) {
                final String value = getTextContent(child);
                final Node factoryIdNode = child.getAttributes().getNamedItem("factory-id");
                if (factoryIdNode == null) {
                    throw new IllegalArgumentException("'factory-id' attribute of 'data-serializable-factory' is required!");
                }
                int factoryId = Integer.parseInt(getTextContent(factoryIdNode));
                serializationConfig.addDataSerializableFactoryClass(factoryId, value);
            }
        }
    }

    protected void fillPortableFactories(Node node, SerializationConfig serializationConfig) {
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String name = cleanNodeName(child);
            if ("portable-factory".equals(name)) {
                final String value = getTextContent(child);
                final Node factoryIdNode = child.getAttributes().getNamedItem("factory-id");
                if (factoryIdNode == null) {
                    throw new IllegalArgumentException("'factory-id' attribute of 'portable-factory' is required!");
                }
                int factoryId = Integer.parseInt(getTextContent(factoryIdNode));
                serializationConfig.addPortableFactoryClass(factoryId, value);
            }
        }
    }

    protected void fillSerializers(final Node node, SerializationConfig serializationConfig) {
        for (org.w3c.dom.Node child : new IterableNodeList(node.getChildNodes())) {
            final String name = cleanNodeName(child);
            final String value = getTextContent(child);
            if ("serializer".equals(name)) {
                SerializerConfig serializerConfig = new SerializerConfig();
                final String typeClassName = getAttribute(child, "type-class");
                final String className = getAttribute(child, "class-name");
                serializerConfig.setTypeClassName(typeClassName);
                serializerConfig.setClassName(className);
                serializationConfig.addSerializerConfig(serializerConfig);
            } else if ("global-serializer".equals(name)) {
                GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig();
                globalSerializerConfig.setClassName(value);
                serializationConfig.setGlobalSerializerConfig(globalSerializerConfig);
            }
        }
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings("DM_BOXED_PRIMITIVE_FOR_PARSING")
    protected void fillOffHeapMemoryConfig(Node node, OffHeapMemoryConfig offHeapMemoryConfig) {
        final NamedNodeMap atts = node.getAttributes();
        final Node enabledNode = atts.getNamedItem("enabled");
        final boolean enabled = enabledNode != null && checkTrue(getTextContent(enabledNode).trim());
        offHeapMemoryConfig.setEnabled(enabled);

        final Node allocTypeNode = atts.getNamedItem("allocator-type");
        final String allocType = getTextContent(allocTypeNode);
        if (allocType != null && !"".equals("")) {
            offHeapMemoryConfig.setAllocatorType(OffHeapMemoryConfig.MemoryAllocatorType.valueOf(upperCaseInternal(allocType)));
        }

        for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes())) {
            final String nodeName = cleanNodeName(n.getNodeName());
            if ("size".equals(nodeName)) {
                final NamedNodeMap attrs = n.getAttributes();
                final String value = getTextContent(attrs.getNamedItem("value"));
                final MemoryUnit unit = MemoryUnit.valueOf(getTextContent(attrs.getNamedItem("unit")));
                MemorySize memorySize = new MemorySize(Long.valueOf(value), unit);
                offHeapMemoryConfig.setSize(memorySize);
            } else if ("min-block-size".equals(nodeName)) {
                String value = getTextContent(n);
                offHeapMemoryConfig.setMinBlockSize(Integer.parseInt(value));
            } else if ("page-size".equals(nodeName)) {
                String value = getTextContent(n);
                offHeapMemoryConfig.setPageSize(Integer.parseInt(value));
            } else if ("metadata-space-percentage".equals(nodeName)) {
                String value = getTextContent(n);
                try {
                    Number percentage = new DecimalFormat("##.#").parse(value);
                    offHeapMemoryConfig.setMetadataSpacePercentage(percentage.floatValue());
                } catch (ParseException e) {
                    LOGGER.info("Metadata space percentage, [" + value
                            + "], is not a proper value. Default value will be used!");
                }
            }
        }
    }


}
