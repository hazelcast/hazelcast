/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.AbstractConfigBuilder.ConfigType;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.util.StringUtil;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.XMLConstants;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.util.StringUtil.upperCaseInternal;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.String.format;

/**
 * Contains Hazelcast XML Configuration helper methods and variables.
 */
@SuppressWarnings("checkstyle:methodcount")
public abstract class AbstractXmlConfigHelper {

    private static final ILogger LOGGER = Logger.getLogger(AbstractXmlConfigHelper.class);

    protected boolean domLevel3 = true;

    final String xmlns = "http://www.hazelcast.com/schema/" + getNamespaceType();

    private final String hazelcastSchemaLocation = getXmlType().name + "-config-" + getReleaseVersion() + ".xsd";

    public static Iterable<Node> childElements(Node node) {
        return new IterableNodeList(node, Node.ELEMENT_NODE);
    }

    public static Iterable<Node> asElementIterable(NodeList list) {
        return new IterableNodeList(list, Node.ELEMENT_NODE);
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

                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    public String getNamespaceType() {
        return getXmlType().name.equals("hazelcast") ? "config" : "client-config";
    }

    protected ConfigType getXmlType() {
        return ConfigType.SERVER;
    }

    protected void schemaValidation(Document doc) throws Exception {
        ArrayList<StreamSource> schemas = new ArrayList<StreamSource>();
        InputStream inputStream = null;
        String schemaLocation = doc.getDocumentElement().getAttribute("xsi:schemaLocation");
        schemaLocation = schemaLocation.replaceAll("^ +| +$| (?= )", "");

        // get every two pair. every pair includes namespace and uri
        String[] xsdLocations = schemaLocation.split("(?<!\\G\\S+)\\s");

        for (String xsdLocation : xsdLocations) {
            if (xsdLocation.isEmpty()) {
                continue;
            }
            String namespace = xsdLocation.split('[' + LINE_SEPARATOR + " ]+")[0];
            String uri = xsdLocation.split('[' + LINE_SEPARATOR + " ]+")[1];

            // if this is hazelcast namespace but location is different log only warning
            if (namespace.equals(xmlns) && !uri.endsWith(hazelcastSchemaLocation)) {
                LOGGER.warning("Name of the hazelcast schema location is incorrect, using default");
            }

            // if this is not hazelcast namespace then try to load from uri
            if (!namespace.equals(xmlns)) {
                inputStream = loadSchemaFile(uri);
                schemas.add(new StreamSource(inputStream));
            }
        }

        // include hazelcast schema
        schemas.add(new StreamSource(getClass().getClassLoader().getResourceAsStream(hazelcastSchemaLocation)));

        // document to InputStream conversion
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Source xmlSource = new DOMSource(doc);
        Result outputTarget = new StreamResult(outputStream);
        TransformerFactory.newInstance().newTransformer().transform(xmlSource, outputTarget);
        InputStream is = new ByteArrayInputStream(outputStream.toByteArray());

        // schema validation
        SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Schema schema = schemaFactory.newSchema(schemas.toArray(new Source[schemas.size()]));
        Validator validator = schema.newValidator();
        try {
            SAXSource source = new SAXSource(new InputSource(is));
            validator.validate(source);
        } catch (Exception e) {
            throw new InvalidConfigurationException(e.getMessage(), e);
        } finally {
            for (StreamSource source : schemas) {
                closeResource(source.getInputStream());
            }
            closeResource(inputStream);
        }
    }

    protected InputStream loadSchemaFile(String schemaLocation) {
        // is resource file
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(schemaLocation);
        // is URL
        if (inputStream == null) {
            try {
                inputStream = new URL(schemaLocation).openStream();
            } catch (Exception e) {
                throw new InvalidConfigurationException("Your xsd schema couldn't be loaded");
            }
        }
        return inputStream;
    }

    protected String getReleaseVersion() {
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
        String[] versionTokens = StringUtil.tokenizeVersionString(buildInfo.getVersion());
        return versionTokens[0] + "." + versionTokens[1];
    }

    protected String xmlToJavaName(final String name) {
        String javaRefName = xmlRefToJavaName(name);
        if (javaRefName != null) {
            return javaRefName;
        }
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

    private String xmlRefToJavaName(final String name) {
        if (name.equals("quorum-ref")) {
            return "quorumName";
        }
        return null;
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

    public static String cleanNodeName(final Node node) {
        final String nodeName = node.getLocalName();
        if (nodeName == null) {
            throw new HazelcastException("Local node name is null for " + node);
        }
        return StringUtil.lowerCaseInternal(nodeName);
    }

    protected static boolean getBooleanValue(final String value) {
        return parseBoolean(StringUtil.lowerCaseInternal(value));
    }

    protected static int getIntegerValue(final String parameterName, final String value) {
        try {
            return Integer.parseInt(value);
        } catch (final NumberFormatException e) {
            throw new InvalidConfigurationException(format("Invalid integer value for parameter %s: %s", parameterName, value));
        }
    }

    protected static int getIntegerValue(final String parameterName, final String value, int defaultValue) {
        if (isNullOrEmpty(value)) {
            return defaultValue;
        }
        return getIntegerValue(parameterName, value);
    }

    protected static long getLongValue(final String parameterName, final String value) {
        try {
            return Long.parseLong(value);
        } catch (final Exception e) {
            throw new InvalidConfigurationException(
                    format("Invalid long integer value for parameter %s: %s", parameterName, value));
        }
    }

    protected static long getLongValue(final String parameterName, final String value, long defaultValue) {
        if (isNullOrEmpty(value)) {
            return defaultValue;
        }
        return getLongValue(parameterName, value);
    }

    protected static double getDoubleValue(final String parameterName, final String value) {
        try {
            return parseDouble(value);
        } catch (final Exception e) {
            throw new InvalidConfigurationException(
                    format("Invalid long integer value for parameter %s: %s", parameterName, value));
        }
    }

    protected static double getDoubleValue(final String parameterName, final String value, double defaultValue) {
        if (isNullOrEmpty(value)) {
            return defaultValue;
        }
        return getDoubleValue(parameterName, value);
    }

    protected String getAttribute(Node node, String attName) {
        final Node attNode = node.getAttributes().getNamedItem(attName);
        if (attNode == null) {
            return null;
        }
        return getTextContent(attNode);
    }

    protected SocketInterceptorConfig parseSocketInterceptorConfig(final Node node) {
        SocketInterceptorConfig socketInterceptorConfig = new SocketInterceptorConfig();
        final NamedNodeMap atts = node.getAttributes();
        final Node enabledNode = atts.getNamedItem("enabled");
        final boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode).trim());
        socketInterceptorConfig.setEnabled(enabled);

        for (Node n : childElements(node)) {
            final String nodeName = cleanNodeName(n);
            if ("class-name".equals(nodeName)) {
                socketInterceptorConfig.setClassName(getTextContent(n).trim());
            } else if ("properties".equals(nodeName)) {
                fillProperties(n, socketInterceptorConfig.getProperties());
            }
        }
        return socketInterceptorConfig;
    }

    protected void fillProperties(final Node node, Properties properties) {
        if (properties == null) {
            return;
        }
        for (Node n : childElements(node)) {
            final String name = cleanNodeName(n);
            final String propertyName = "property".equals(name)
                    ? getTextContent(n.getAttributes().getNamedItem("name")).trim()
                    // old way - probably should be deprecated
                    : name;
            final String value = getTextContent(n).trim();
            properties.setProperty(propertyName, value);
        }
    }

    protected void fillProperties(final Node node, Map<String, Comparable> properties) {
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
                propertyName = getTextContent(n.getAttributes().getNamedItem("name")).trim();
            } else {
                // old way - probably should be deprecated
                propertyName = name;
            }
            final String value = getTextContent(n).trim();
            properties.put(propertyName, value);
        }
    }

    protected SerializationConfig parseSerialization(final Node node) {
        SerializationConfig serializationConfig = new SerializationConfig();
        for (Node child : childElements(node)) {
            final String name = cleanNodeName(child);
            if ("portable-version".equals(name)) {
                String value = getTextContent(child);
                serializationConfig.setPortableVersion(getIntegerValue(name, value));
            } else if ("check-class-def-errors".equals(name)) {
                String value = getTextContent(child);
                serializationConfig.setCheckClassDefErrors(getBooleanValue(value));
            } else if ("use-native-byte-order".equals(name)) {
                serializationConfig.setUseNativeByteOrder(getBooleanValue(getTextContent(child)));
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
                serializationConfig.setEnableCompression(getBooleanValue(getTextContent(child)));
            } else if ("enable-shared-object".equals(name)) {
                serializationConfig.setEnableSharedObject(getBooleanValue(getTextContent(child)));
            } else if ("allow-unsafe".equals(name)) {
                serializationConfig.setAllowUnsafe(getBooleanValue(getTextContent(child)));
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
        for (Node child : childElements(node)) {
            final String name = cleanNodeName(child);
            if ("data-serializable-factory".equals(name)) {
                final String value = getTextContent(child);
                final Node factoryIdNode = child.getAttributes().getNamedItem("factory-id");
                if (factoryIdNode == null) {
                    throw new IllegalArgumentException(
                            "'factory-id' attribute of 'data-serializable-factory' is required!");
                }
                int factoryId = Integer.parseInt(getTextContent(factoryIdNode));
                serializationConfig.addDataSerializableFactoryClass(factoryId, value);
            }
        }
    }

    protected void fillPortableFactories(Node node, SerializationConfig serializationConfig) {
        for (Node child : childElements(node)) {
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
        for (Node child : childElements(node)) {
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
                String attrValue = getAttribute(child, "override-java-serialization");
                boolean overrideJavaSerialization = attrValue != null && getBooleanValue(attrValue.trim());
                globalSerializerConfig.setOverrideJavaSerialization(overrideJavaSerialization);
                serializationConfig.setGlobalSerializerConfig(globalSerializerConfig);
            }
        }
    }

    protected void fillNativeMemoryConfig(Node node, NativeMemoryConfig nativeMemoryConfig) {
        final NamedNodeMap atts = node.getAttributes();
        final Node enabledNode = atts.getNamedItem("enabled");
        final boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode).trim());
        nativeMemoryConfig.setEnabled(enabled);

        final Node allocTypeNode = atts.getNamedItem("allocator-type");
        final String allocType = getTextContent(allocTypeNode);
        if (allocType != null && !"".equals(allocType)) {
            nativeMemoryConfig.setAllocatorType(
                    NativeMemoryConfig.MemoryAllocatorType.valueOf(upperCaseInternal(allocType)));
        }

        for (Node n : childElements(node)) {
            final String nodeName = cleanNodeName(n);
            if ("size".equals(nodeName)) {
                final NamedNodeMap attrs = n.getAttributes();
                final String value = getTextContent(attrs.getNamedItem("value"));
                final MemoryUnit unit = MemoryUnit.valueOf(getTextContent(attrs.getNamedItem("unit")));
                MemorySize memorySize = new MemorySize(Long.parseLong(value), unit);
                nativeMemoryConfig.setSize(memorySize);
            } else if ("min-block-size".equals(nodeName)) {
                String value = getTextContent(n);
                nativeMemoryConfig.setMinBlockSize(Integer.parseInt(value));
            } else if ("page-size".equals(nodeName)) {
                String value = getTextContent(n);
                nativeMemoryConfig.setPageSize(Integer.parseInt(value));
            } else if ("metadata-space-percentage".equals(nodeName)) {
                String value = getTextContent(n);
                nativeMemoryConfig.setMetadataSpacePercentage(Float.parseFloat(value));
            }
        }
    }
}
