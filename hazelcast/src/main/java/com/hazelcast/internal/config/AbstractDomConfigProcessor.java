/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.config;

import com.hazelcast.config.AbstractBaseFactoryWithPropertiesConfig;
import com.hazelcast.config.AbstractFactoryWithPropertiesConfig;
import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.CompactSerializationConfigAccessor;
import com.hazelcast.internal.diagnostics.DiagnosticsConfig;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.InstanceTrackingConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.PersistentMemoryConfig;
import com.hazelcast.config.PersistentMemoryDirectoryConfig;
import com.hazelcast.config.PersistentMemoryMode;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.config.UserCodeNamespacesConfig;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.internal.diagnostics.DiagnosticsOutputType;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.memory.Capacity;
import com.hazelcast.memory.MemoryUnit;
import org.w3c.dom.Node;

import javax.annotation.Nonnull;

import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.internal.config.DomConfigHelper.getIntegerValue;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmptyAfterTrim;
import static com.hazelcast.internal.util.StringUtil.upperCaseInternal;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;

/**
 * Base class of the config processors working from W3C DOM objects
 */
public abstract class AbstractDomConfigProcessor implements DomConfigProcessor {

    /**
     * Set collecting already seen elements. Used to detect duplicates in
     * the configurations.
     */
    protected final Set<String> occurrenceSet = new HashSet<>();

    /**
     * Enables strict parsing mode in which config entries are parsed as-is
     * When disabled, dashes are ignored
     */
    protected final boolean strict;

    /**
     * Tells whether the traversed DOM is a level 3 one
     */
    final boolean domLevel3;

    protected AbstractDomConfigProcessor(boolean domLevel3) {
        this.domLevel3 = domLevel3;
        this.strict = true;
    }

    protected AbstractDomConfigProcessor(boolean domLevel3, boolean strict) {
        this.domLevel3 = domLevel3;
        this.strict = strict;
    }

    @Nonnull
    protected String getTextContent(Node node) {
        return DomConfigHelper.getTextContent(node, domLevel3).trim();
    }

    protected String getAttribute(Node node, String attName) {
        if (strict) {
            return DomConfigHelper.getAttribute(node, attName, domLevel3);
        } else {
            String value = DomConfigHelper.getAttribute(node, attName, domLevel3);
            return value != null
              ? value
              : DomConfigHelper.getAttribute(node, attName.replace("-", ""), domLevel3);
        }
    }

    protected Node getNamedItemNode(final Node node, String attrName) {
        if (strict) {
            return node.getAttributes().getNamedItem(attrName);
        } else {
            Node attrNode = node.getAttributes().getNamedItem(attrName);
            return attrNode != null
              ? attrNode
              : node.getAttributes().getNamedItem(attrName.replace("-", ""));
        }
    }

    protected boolean matches(String config1, String config2) {
        return strict
          ? config1 != null && config1.equals(config2)
          : ConfigUtils.matches(config1, config2);
    }

    protected void fillProperties(Node node, Map<String, Comparable> properties) {
        DomConfigHelper.fillProperties(node, properties, domLevel3);
    }

    protected void fillProperties(Node node, Properties properties) {
        DomConfigHelper.fillProperties(node, properties, domLevel3);
    }

    protected SocketInterceptorConfig parseSocketInterceptorConfig(final Node node) {
        SocketInterceptorConfig socketInterceptorConfig = new SocketInterceptorConfig();
        final Node enabledNode = getNamedItemNode(node, "enabled");
        final boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode).trim());
        socketInterceptorConfig.setEnabled(enabled);

        for (Node n : childElements(node)) {
            final String nodeName = cleanNodeName(n);
            if (matches("class-name", nodeName)) {
                socketInterceptorConfig.setClassName(getTextContent(n).trim());
            } else if (matches("properties", nodeName)) {
                fillProperties(n, socketInterceptorConfig.getProperties());
            }
        }
        return socketInterceptorConfig;
    }

    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    protected SerializationConfig parseSerialization(final Node node) {
        SerializationConfig serializationConfig = new SerializationConfig();
        for (Node child : childElements(node)) {
            final String name = cleanNodeName(child);
            if (matches("portable-version", name)) {
                String value = getTextContent(child);
                serializationConfig.setPortableVersion(getIntegerValue(name, value));
            } else if (matches("check-class-def-errors", name)) {
                String value = getTextContent(child);
                serializationConfig.setCheckClassDefErrors(getBooleanValue(value));
            } else if (matches("use-native-byte-order", name)) {
                serializationConfig.setUseNativeByteOrder(getBooleanValue(getTextContent(child)));
            } else if (matches("byte-order", name)) {
                String value = getTextContent(child);
                ByteOrder byteOrder = null;
                if (ByteOrder.BIG_ENDIAN.toString().equals(value)) {
                    byteOrder = ByteOrder.BIG_ENDIAN;
                } else if (ByteOrder.LITTLE_ENDIAN.toString().equals(value)) {
                    byteOrder = ByteOrder.LITTLE_ENDIAN;
                }
                serializationConfig.setByteOrder(byteOrder != null ? byteOrder : ByteOrder.BIG_ENDIAN);
            } else if (matches("enable-compression", name)) {
                serializationConfig.setEnableCompression(getBooleanValue(getTextContent(child)));
            } else if (matches("enable-shared-object", name)) {
                serializationConfig.setEnableSharedObject(getBooleanValue(getTextContent(child)));
            } else if (matches("allow-unsafe", name)) {
                serializationConfig.setAllowUnsafe(getBooleanValue(getTextContent(child)));
            } else if (matches("allow-override-default-serializers", name)) {
                serializationConfig.setAllowOverrideDefaultSerializers(getBooleanValue(getTextContent(child)));
            } else if (matches("data-serializable-factories", name)) {
                fillDataSerializableFactories(child, serializationConfig);
            } else if (matches("portable-factories", name)) {
                fillPortableFactories(child, serializationConfig);
            } else if (matches("serializers", name)) {
                fillSerializers(child, serializationConfig);
            } else if (matches("java-serialization-filter", name)) {
                fillJavaSerializationFilter(child, serializationConfig);
            } else if (matches("compact-serialization", name)) {
                handleCompactSerialization(child, serializationConfig);
            }
        }
        return serializationConfig;
    }

    protected void handleDiagnostics(Node node, DiagnosticsConfig diagnosticsConfig) {

        diagnosticsConfig.setEnabled(getBooleanValue(getAttribute(node, "enabled")));

        for (Node n : childElements(node)) {
            String name = cleanNodeName(n);
            if (matches("max-rolled-file-size-in-mb", name)) {
                diagnosticsConfig.setMaxRolledFileSizeInMB(Float.parseFloat(n.getTextContent()));
            } else if (matches("max-rolled-file-count", name)) {
                diagnosticsConfig.setMaxRolledFileCount(parseInt(n.getTextContent()));
            } else if (matches("include-epoch-time", name)) {
                diagnosticsConfig.setIncludeEpochTime(parseBoolean(n.getTextContent()));
            } else if (matches("log-directory", name)) {
                diagnosticsConfig.setLogDirectory(n.getTextContent());
            } else if (matches("file-name-prefix", name)) {
                diagnosticsConfig.setFileNamePrefix(n.getTextContent());
            } else if (matches("output-type", name)) {
                diagnosticsConfig.setOutputType(DiagnosticsOutputType.valueOf(n.getTextContent()));
            } else if (matches("auto-off-timer-in-minutes", name)) {
                diagnosticsConfig.setAutoOffDurationInMinutes(parseInt(n.getTextContent()));
            } else if (matches("plugin-properties", name)) {
                Map<String, Comparable> rawProperties = new HashMap<>();
                fillProperties(n, rawProperties);
                rawProperties
                        .entrySet()
                        .forEach(entry -> diagnosticsConfig.getPluginProperties()
                                .put(entry.getKey(), (String) entry.getValue()));
            }
        }
    }

    protected void handleCompactSerialization(Node node, SerializationConfig serializationConfig) {
        CompactSerializationConfig compactSerializationConfig = serializationConfig.getCompactSerializationConfig();
        for (Node child : childElements(node)) {
            String name = cleanNodeName(child);
            if (matches("serializers", name)) {
                fillCompactSerializers(child, compactSerializationConfig);
            } else if (matches("classes", name)) {
                fillCompactSerializableClasses(child, compactSerializationConfig);
            }
        }
    }

    protected void fillCompactSerializers(Node node, CompactSerializationConfig compactSerializationConfig) {
        for (Node child : childElements(node)) {
            String name = cleanNodeName(child);
            if (matches("serializer", name)) {
                String serializerClassName = getTextContent(child);
                CompactSerializationConfigAccessor.registerSerializer(compactSerializationConfig, serializerClassName);
            }
        }
    }

    protected void fillCompactSerializableClasses(Node node, CompactSerializationConfig compactSerializationConfig) {
        for (Node child : childElements(node)) {
            String name = cleanNodeName(child);
            if (matches("class", name)) {
                String compactSerializableClassName = getTextContent(child);
                CompactSerializationConfigAccessor.registerClass(compactSerializationConfig,
                        compactSerializableClassName);
            }
        }
    }

    protected void fillDataSerializableFactories(Node node, SerializationConfig serializationConfig) {
        for (Node child : childElements(node)) {
            final String name = cleanNodeName(child);
            if (matches("data-serializable-factory", name)) {
                final String value = getTextContent(child);
                final Node factoryIdNode = getNamedItemNode(child, "factory-id");
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
            if (matches("portable-factory", name)) {
                final String value = getTextContent(child);
                final Node factoryIdNode = getNamedItemNode(child, "factory-id");
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
            if (matches("serializer", name)) {
                SerializerConfig serializerConfig = new SerializerConfig();
                final String typeClassName = getAttribute(child, "type-class");
                final String className = getAttribute(child, "class-name");
                serializerConfig.setTypeClassName(typeClassName);
                serializerConfig.setClassName(className);
                serializationConfig.addSerializerConfig(serializerConfig);
            } else if (matches("global-serializer", name)) {
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
        serializationConfig.setJavaSerializationFilterConfig(getJavaFilter(node));
    }

    protected void fillJavaSerializationFilter(final Node node, UserCodeNamespacesConfig userCodeNamespacesConfig) {
        userCodeNamespacesConfig.setClassFilterConfig(getJavaFilter(node));
    }

    JavaSerializationFilterConfig getJavaFilter(final Node node) {
        JavaSerializationFilterConfig filterConfig = new JavaSerializationFilterConfig();
        Node defaultsDisabledNode = getNamedItemNode(node, "defaults-disabled");
        boolean defaultsDisabled = defaultsDisabledNode != null && getBooleanValue(getTextContent(defaultsDisabledNode));
        filterConfig.setDefaultsDisabled(defaultsDisabled);
        for (Node child : childElements(node)) {
            final String name = cleanNodeName(child);
            if (matches("blacklist", name)) {
                ClassFilter list = parseClassFilterList(child);
                filterConfig.setBlacklist(list);
            } else if (matches("whitelist", name)) {
                ClassFilter list = parseClassFilterList(child);
                filterConfig.setWhitelist(list);
            }
        }
        return filterConfig;
    }

    protected ClassFilter parseClassFilterList(Node node) {
        ClassFilter list = new ClassFilter();
        for (Node child : childElements(node)) {
            final String name = cleanNodeName(child);
            if (matches("class", name)) {
                list.addClasses(getTextContent(child));
            } else if (matches("package", name)) {
                list.addPackages(getTextContent(child));
            } else if (matches("prefix", name)) {
                list.addPrefixes(getTextContent(child));
            }
        }
        return list;
    }

    protected SSLConfig parseSslConfig(Node node) {
        return fillFactoryWithPropertiesConfig(node, new SSLConfig());
    }

    protected <T extends AbstractBaseFactoryWithPropertiesConfig<?>> T fillBaseFactoryWithPropertiesConfig(Node node,
            T factoryConfig) {
        for (Node n : childElements(node)) {
            String nodeName = cleanNodeName(n);
            if (matches("factory-class-name", nodeName)) {
                factoryConfig.setFactoryClassName(getTextContent(n));
            } else if (matches("properties", nodeName)) {
                fillProperties(n, factoryConfig.getProperties());
            }
        }
        return factoryConfig;
    }

    protected <T extends AbstractFactoryWithPropertiesConfig<?>> T fillFactoryWithPropertiesConfig(Node node, T factoryConfig) {
        fillBaseFactoryWithPropertiesConfig(node, factoryConfig);
        Node enabledNode = getNamedItemNode(node, "enabled");
        boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode));
        factoryConfig.setEnabled(enabled);
        return factoryConfig;
    }

    protected void fillNativeMemoryConfig(Node node, NativeMemoryConfig nativeMemoryConfig) {
        final Node enabledNode = getNamedItemNode(node, "enabled");
        final boolean enabled = enabledNode != null && getBooleanValue(getTextContent(enabledNode).trim());
        nativeMemoryConfig.setEnabled(enabled);

        final Node allocTypeNode = getNamedItemNode(node, "allocator-type");
        final String allocType = getTextContent(allocTypeNode);
        if (allocType != null && !"".equals(allocType)) {
            nativeMemoryConfig.setAllocatorType(
                    NativeMemoryConfig.MemoryAllocatorType.valueOf(upperCaseInternal(allocType)));
        }

        for (Node n : childElements(node)) {
            final String nodeName = cleanNodeName(n);
            if (matches("size", nodeName)) {
                nativeMemoryConfig.setCapacity(createCapacity(n));
            } else if (matches("capacity", nodeName)) {
                nativeMemoryConfig.setCapacity(createCapacity(n));
            } else if (matches("min-block-size", nodeName)) {
                String value = getTextContent(n);
                nativeMemoryConfig.setMinBlockSize(Integer.parseInt(value));
            } else if (matches("page-size", nodeName)) {
                String value = getTextContent(n);
                nativeMemoryConfig.setPageSize(Integer.parseInt(value));
            } else if (matches("metadata-space-percentage", nodeName)) {
                String value = getTextContent(n);
                nativeMemoryConfig.setMetadataSpacePercentage(Float.parseFloat(value));
            } else if (matches("persistent-memory-directory", nodeName)) {
                PersistentMemoryConfig pmemConfig = nativeMemoryConfig.getPersistentMemoryConfig();
                pmemConfig.addDirectoryConfig(new PersistentMemoryDirectoryConfig(getTextContent(n).trim()));
                // we enable the persistent memory configuration for legacy reasons
                pmemConfig.setEnabled(true);
            } else if (matches("persistent-memory", nodeName)) {
                handlePersistentMemoryConfig(nativeMemoryConfig.getPersistentMemoryConfig(), n);
            }
        }
    }

    protected Capacity createCapacity(Node node) {
        final String value = getTextContent(getNamedItemNode(node, "value"));
        final MemoryUnit unit = MemoryUnit.valueOf(getTextContent(getNamedItemNode(node, "unit")));
        return new Capacity(Long.parseLong(value), unit);
    }

    private void handlePersistentMemoryConfig(PersistentMemoryConfig persistentMemoryConfig, Node node) {
        Node enabledNode = getNamedItemNode(node, "enabled");
        if (enabledNode != null) {
            boolean enabled = getBooleanValue(getTextContent(enabledNode));
            persistentMemoryConfig.setEnabled(enabled);
        }

        final Node modeNode = getNamedItemNode(node, "mode");
        final String modeStr = getTextContent(modeNode);
        PersistentMemoryMode mode = PersistentMemoryMode.MOUNTED;
        if (!StringUtil.isNullOrEmptyAfterTrim(modeStr)) {
            try {
                mode = PersistentMemoryMode.valueOf(modeStr);
                persistentMemoryConfig.setMode(mode);
            } catch (Exception ex) {
                throw new InvalidConfigurationException("Invalid 'mode' for 'persistent-memory': " + modeStr);
            }
        }

        for (Node parent : childElements(node)) {
            final String nodeName = cleanNodeName(parent);
            if (matches("directories", nodeName)) {
                if (PersistentMemoryMode.SYSTEM_MEMORY == mode) {
                    throw new InvalidConfigurationException("Directories for 'persistent-memory' should only be"
                            + " defined if the 'mode' is set to '" + PersistentMemoryMode.MOUNTED.name() + "'");
                }

                for (Node dirNode : childElements(parent)) {
                    handlePersistentMemoryDirectory(persistentMemoryConfig, dirNode);
                }
            }
        }
    }

    protected void handlePersistentMemoryDirectory(PersistentMemoryConfig persistentMemoryConfig, Node dirNode) {
        final String childNodeName = cleanNodeName(dirNode);
        if (matches("directory", childNodeName)) {
            Node numaNodeIdNode = getNamedItemNode(dirNode, "numa-node");
            int numaNodeId = numaNodeIdNode != null
                    ? getIntegerValue("numa-node", getTextContent(numaNodeIdNode))
                    : -1;
            String directory = getTextContent(dirNode).trim();
            persistentMemoryConfig.addDirectoryConfig(new PersistentMemoryDirectoryConfig(directory, numaNodeId));
        }
    }

    protected void handleJaasAuthentication(RealmConfig realmConfig, Node node) {
        JaasAuthenticationConfig jaasAuthenticationConfig = new JaasAuthenticationConfig();
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("login-module", nodeName)) {
                jaasAuthenticationConfig.addLoginModuleConfig(handleLoginModule(child));
            }
        }
        realmConfig.setJaasAuthenticationConfig(jaasAuthenticationConfig);
    }

    protected LoginModuleConfig handleLoginModule(Node node) {
        Node classNameNode = getNamedItemNode(node, "class-name");
        String className = getTextContent(classNameNode);
        Node usageNode = getNamedItemNode(node, "usage");
        LoginModuleConfig.LoginModuleUsage usage =
                usageNode != null ? LoginModuleConfig.LoginModuleUsage.get(getTextContent(usageNode))
                        : LoginModuleConfig.LoginModuleUsage.REQUIRED;
        LoginModuleConfig moduleConfig = new LoginModuleConfig(className, usage);
        for (Node child : childElements(node)) {
            String nodeName = cleanNodeName(child);
            if (matches("properties", nodeName)) {
                fillProperties(child, moduleConfig.getProperties());
                break;
            }
        }
        return moduleConfig;
    }

    protected void handleInstanceTracking(Node node, InstanceTrackingConfig trackingConfig) {
        Node attrEnabled = getNamedItemNode(node, "enabled");
        String textContent = getTextContent(attrEnabled);
        if (!isNullOrEmptyAfterTrim(textContent)) {
            trackingConfig.setEnabled(getBooleanValue(textContent));
        }

        for (Node n : childElements(node)) {
            final String name = cleanNodeName(n);
            if (matches("file-name", name)) {
                trackingConfig.setFileName(getTextContent(n));
            } else if (matches("format-pattern", name)) {
                trackingConfig.setFormatPattern(getTextContent(n));
            }
        }
    }
}
