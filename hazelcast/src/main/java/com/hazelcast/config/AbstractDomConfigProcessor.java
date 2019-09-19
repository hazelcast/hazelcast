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

import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.config.DomConfigHelper.childElements;
import static com.hazelcast.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.config.DomConfigHelper.getIntegerValue;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;

/**
 * Base class of the config processors working from W3C DOM objects
 */
public abstract class AbstractDomConfigProcessor implements DomConfigProcessor {

    /**
     * Set collecting already seen elements. Used to detect duplicates in
     * the configurations.
     */
    protected final Set<String> occurrenceSet = new HashSet<String>();

    /**
     * Tells whether the traversed DOM is a level 3 one
     */
    private final boolean domLevel3;

    protected AbstractDomConfigProcessor(boolean domLevel3) {
        this.domLevel3 = domLevel3;
    }

    protected String getTextContent(Node node) {
        return DomConfigHelper.getTextContent(node, domLevel3);
    }

    protected String getAttribute(Node node, String attName) {
        return DomConfigHelper.getAttribute(node, attName, domLevel3);
    }

    protected void fillProperties(Node node, Map<String, Comparable> properties) {
        DomConfigHelper.fillProperties(node, properties, domLevel3);
    }

    protected void fillProperties(Node node, Properties properties) {
        DomConfigHelper.fillProperties(node, properties, domLevel3);
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
            } else if ("java-serialization-filter".equals(name)) {
                fillJavaSerializationFilter(child, serializationConfig);
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

    protected void fillJavaSerializationFilter(final Node node, SerializationConfig serializationConfig) {
        JavaSerializationFilterConfig filterConfig = new JavaSerializationFilterConfig();
        serializationConfig.setJavaSerializationFilterConfig(filterConfig);
        Node defaultsDisabledNode = node.getAttributes().getNamedItem("defaults-disabled");
        boolean defaultsDisabled = defaultsDisabledNode != null && getBooleanValue(getTextContent(defaultsDisabledNode));
        filterConfig.setDefaultsDisabled(defaultsDisabled);
        for (Node child : childElements(node)) {
            final String name = cleanNodeName(child);
            if ("blacklist".equals(name)) {
                ClassFilter list = parseClassFilterList(child);
                filterConfig.setBlacklist(list);
            } else if ("whitelist".equals(name)) {
                ClassFilter list = parseClassFilterList(child);
                filterConfig.setWhitelist(list);
            }
        }
    }

    protected ClassFilter parseClassFilterList(Node node) {
        ClassFilter list = new ClassFilter();
        for (Node child : childElements(node)) {
            final String name = cleanNodeName(child);
            if ("class".equals(name)) {
                list.addClasses(getTextContent(child));
            } else if ("package".equals(name)) {
                list.addPackages(getTextContent(child));
            } else if ("prefix".equals(name)) {
                list.addPrefixes(getTextContent(child));
            }
        }
        return list;
    }

    protected SSLConfig parseSslConfig(Node node) {
        SSLConfig sslConfig = new SSLConfig();
        NamedNodeMap atts = node.getAttributes();
        Node enabledNode = atts.getNamedItem("enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode));
        sslConfig.setEnabled(enabled);

        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if ("factory-class-name".equals(nodeName)) {
                sslConfig.setFactoryClassName(getTextContent(n));
            } else if ("properties".equals(nodeName)) {
                fillProperties(n, sslConfig.getProperties());
            }
        }
        return sslConfig;
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
            } else if ("persistent-memory-directory".equals(nodeName)) {
                nativeMemoryConfig.setPersistentMemoryDirectory(getTextContent(n).trim());
            }
        }
    }

}
