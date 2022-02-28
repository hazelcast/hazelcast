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

package com.hazelcast.client.config.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ClientSecurityConfig;
import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.PersistentMemoryConfig;
import com.hazelcast.config.PersistentMemoryDirectoryConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.TokenIdentityConfig;
import com.hazelcast.internal.util.StringUtil;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.nio.ByteOrder;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.config.security.TokenEncoding.getTokenEncoding;
import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.internal.config.DomConfigHelper.getIntegerValue;

public class YamlClientDomConfigProcessor extends ClientDomConfigProcessor {
    public YamlClientDomConfigProcessor(boolean domLevel3, ClientConfig clientConfig) {
        super(domLevel3, clientConfig, new QueryCacheYamlConfigBuilderHelper());
    }

    public YamlClientDomConfigProcessor(boolean domLevel3, ClientConfig clientConfig, boolean strict) {
        super(domLevel3, clientConfig, new QueryCacheYamlConfigBuilderHelper(strict), strict);
    }

    @Override
    protected void handleClusterMembers(Node node, ClientNetworkConfig clientNetworkConfig) {
        for (Node child : childElements(node)) {
            clientNetworkConfig.addAddress(getTextContent(child));
        }
    }

    @Override
    protected void handleOutboundPorts(Node child, ClientNetworkConfig clientNetworkConfig) {
        for (Node n : childElements(child)) {
            String value = getTextContent(n);
            clientNetworkConfig.addOutboundPortDefinition(value);
        }
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity"})
    protected SerializationConfig parseSerialization(final Node node) {
        SerializationConfig serializationConfig = new SerializationConfig();
        for (Node child : childElements(node)) {
            final String name = cleanNodeName(child);
            if (matches("portable-version", name)) {
                serializationConfig.setPortableVersion(getIntegerValue(name, getTextContent(child)));
            } else if (matches("check-class-def-errors", name)) {
                serializationConfig.setCheckClassDefErrors(getBooleanValue(getTextContent(child)));
            } else if (matches("use-native-byte-order", name)) {
                serializationConfig.setUseNativeByteOrder(getBooleanValue(getTextContent(child)));
            } else if (matches("byte-order", name)) {
                ByteOrder byteOrder = null;
                if (ByteOrder.BIG_ENDIAN.toString().equals(getTextContent(child))) {
                    byteOrder = ByteOrder.BIG_ENDIAN;
                } else if (ByteOrder.LITTLE_ENDIAN.toString().equals(getTextContent(child))) {
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
            } else if (matches("global-serializer", name)) {
                fillGlobalSerializer(child, serializationConfig);
            } else if (matches("java-serialization-filter", name)) {
                fillJavaSerializationFilter(child, serializationConfig);
            } else if (matches("compact-serialization", name)) {
                handleCompactSerialization(child, serializationConfig);
            }
        }
        return serializationConfig;
    }

    @Override
    protected String parseCustomLoadBalancerClassName(Node node) {
        return getAttribute(node, "class-name");
    }

    @Override
    protected void handleCompactSerialization(Node node, SerializationConfig serializationConfig) {
        CompactSerializationConfig compactSerializationConfig = serializationConfig.getCompactSerializationConfig();
        for (Node child : childElements(node)) {
            String name = cleanNodeName(child);
            if (matches("enabled", name)) {
                boolean enabled = getBooleanValue(getTextContent(child));
                compactSerializationConfig.setEnabled(enabled);
            } else if (matches("registered-classes", name)) {
                fillCompactSerializableClasses(child, compactSerializationConfig);
            }
        }
    }

    @Override
    protected void fillCompactSerializableClasses(Node node, CompactSerializationConfig compactSerializationConfig) {
        for (Node child : childElements(node)) {
            String className = getAttribute(child, "class");
            String typeName = getAttribute(child, "type-name");
            String serializerClassName = getAttribute(child, "serializer");
            registerCompactSerializableClass(compactSerializationConfig, className, typeName, serializerClassName);
        }
    }

    private void fillGlobalSerializer(Node child, SerializationConfig serializationConfig) {
        GlobalSerializerConfig globalSerializerConfig = new GlobalSerializerConfig();
        String attrClassName = getAttribute(child, "class-name");
        String attrOverrideJavaSerialization = getAttribute(child, "override-java-serialization");
        boolean overrideJavaSerialization =
                attrOverrideJavaSerialization != null && getBooleanValue(attrOverrideJavaSerialization.trim());
        globalSerializerConfig.setClassName(attrClassName);
        globalSerializerConfig.setOverrideJavaSerialization(overrideJavaSerialization);
        serializationConfig.setGlobalSerializerConfig(globalSerializerConfig);
    }

    @Override
    protected void fillSerializers(Node node, SerializationConfig serializationConfig) {
        for (Node child : childElements(node)) {
            SerializerConfig serializerConfig = new SerializerConfig();
            final String typeClassName = getAttribute(child, "type-class");
            final String className = getAttribute(child, "class-name");
            serializerConfig.setTypeClassName(typeClassName);
            serializerConfig.setClassName(className);
            serializationConfig.addSerializerConfig(serializerConfig);
        }
    }

    @Override
    protected void fillDataSerializableFactories(Node node, SerializationConfig serializationConfig) {
        for (Node child : childElements(node)) {
            final Node factoryIdNode = getNamedItemNode(child, "factory-id");
            final Node classNameNode = getNamedItemNode(child, "class-name");
            if (factoryIdNode == null) {
                throw new IllegalArgumentException(
                        "'factory-id' attribute of 'data-serializable-factory' is required!");
            }
            if (classNameNode == null) {
                throw new IllegalArgumentException(
                        "'class-name' attribute of 'data-serializable-factory' is required!");
            }
            int factoryId = Integer.parseInt(getTextContent(factoryIdNode));
            String className = getTextContent(classNameNode);
            serializationConfig.addDataSerializableFactoryClass(factoryId, className);
        }
    }

    @Override
    protected void fillPortableFactories(Node node, SerializationConfig serializationConfig) {
        for (Node child : childElements(node)) {
            final Node factoryIdNode = getNamedItemNode(child, "factory-id");
            final Node classNameNode = getNamedItemNode(child, "class-name");
            if (factoryIdNode == null) {
                throw new IllegalArgumentException("'factory-id' attribute of 'portable-factory' is required!");
            }
            if (classNameNode == null) {
                throw new IllegalArgumentException("'class-name' attribute of 'portable-factory' is required!");
            }
            int factoryId = Integer.parseInt(getTextContent(factoryIdNode));
            String className = getTextContent(classNameNode);
            serializationConfig.addPortableFactoryClass(factoryId, className);
        }
    }

    @Override
    protected ClassFilter parseClassFilterList(Node node) {
        ClassFilter list = new ClassFilter();
        for (Node typeNode : childElements(node)) {
            final String name = cleanNodeName(typeNode);
            if (matches("class", name)) {
                for (Node classNode : childElements(typeNode)) {
                    list.addClasses(getTextContent(classNode));
                }
            } else if (matches("package", name)) {
                for (Node packageNode : childElements(typeNode)) {
                    list.addPackages(getTextContent(packageNode));
                }
            } else if (matches("prefix", name)) {
                for (Node prefixNode : childElements(typeNode)) {
                    list.addPrefixes(getTextContent(prefixNode));
                }
            }
        }
        return list;
    }

    @Override
    protected void handleUserCodeDeploymentNode(ClientUserCodeDeploymentConfig userCodeDeploymentConfig, Node child) {
        String childNodeName = cleanNodeName(child);
        if (matches("classnames", childNodeName)) {
            for (Node classNameNode : childElements(child)) {
                userCodeDeploymentConfig.addClass(getTextContent(classNameNode));
            }
        } else if (matches("jarpaths", childNodeName)) {
            for (Node jarPathNode : childElements(child)) {
                userCodeDeploymentConfig.addJar(getTextContent(jarPathNode));
            }
        }
    }

    @Override
    protected void handleListeners(Node node) {
        for (Node child : childElements(node)) {
            String className = getTextContent(child);
            clientConfig.addListenerConfig(new ListenerConfig(className));
        }
    }

    @Override
    protected void handleNearCache(Node node) {
        for (Node child : childElements(node)) {
            handleNearCacheNode(child);
        }
    }

    @Override
    protected void handleReliableTopic(Node node) {
        for (Node child : childElements(node)) {
            handleReliableTopicNode(child);
        }
    }

    @Override
    protected void handleFlakeIdGenerator(Node node) {
        for (Node child : childElements(node)) {
            handleFlakeIdGeneratorNode(child);
        }
    }

    @Override
    protected void handleProxyFactoryNode(Node child) {
        handleProxyFactory(child);
    }

    @Override
    protected String getName(Node node) {
        return node.getNodeName();
    }

    @Override
    protected void fillProperties(Node node, Map<String, Comparable> properties) {
        NodeList childNodes = node.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node childNode = childNodes.item(i);
            properties.put(childNode.getNodeName(), childNode.getNodeValue());
        }
    }

    @Override
    protected void fillProperties(Node node, Properties properties) {
        NodeList childNodes = node.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node childNode = childNodes.item(i);
            properties.put(childNode.getNodeName(), childNode.getNodeValue());
        }
    }

    @Override
    protected void handleDiscoveryStrategies(Node node, ClientNetworkConfig clientNetworkConfig) {
        DiscoveryConfig discoveryConfig = clientNetworkConfig.getDiscoveryConfig();
        for (Node child : childElements(node)) {
            String name = cleanNodeName(child);
            if (matches("discovery-strategies", name)) {
                handleDiscoveryStrategiesNode(child, discoveryConfig);
            } else if (matches("node-filter", name)) {
                handleDiscoveryNodeFilter(child, discoveryConfig);
            }
        }
    }

    private void handleDiscoveryStrategiesNode(Node node, DiscoveryConfig discoveryConfig) {
        for (Node child : childElements(node)) {
            handleDiscoveryStrategy(child, discoveryConfig);
        }
    }

    @Override
    protected void handleTokenIdentity(ClientSecurityConfig clientSecurityConfig, Node node) {
        clientSecurityConfig.setTokenIdentityConfig(new TokenIdentityConfig(
                getTokenEncoding(getAttribute(node, "encoding")), getAttribute(node, "value")));
    }

    @Override
    protected void handleRealms(Node node, ClientSecurityConfig clientSecurityConfig) {
        for (Node child : childElements(node)) {
            handleRealm(child, clientSecurityConfig);
        }
    }

    @Override
    protected void handleJaasAuthentication(RealmConfig realmConfig, Node node) {
        JaasAuthenticationConfig jaasAuthenticationConfig = new JaasAuthenticationConfig();
        for (Node child : childElements(node)) {
            jaasAuthenticationConfig.addLoginModuleConfig(handleLoginModule(child));
        }
        realmConfig.setJaasAuthenticationConfig(jaasAuthenticationConfig);
    }

    @Override
    protected void handlePersistentMemoryDirectory(PersistentMemoryConfig persistentMemoryConfig, Node dirNode) {
        String directory = getTextContent(getNamedItemNode(dirNode, "directory"));
        String numaNodeIdStr = getTextContent(getNamedItemNode(dirNode, "numa-node"));
        if (!StringUtil.isNullOrEmptyAfterTrim(numaNodeIdStr)) {
            int numaNodeId = getIntegerValue("numa-node", numaNodeIdStr);
            persistentMemoryConfig.addDirectoryConfig(new PersistentMemoryDirectoryConfig(directory, numaNodeId));
        } else {
            persistentMemoryConfig.addDirectoryConfig(new PersistentMemoryDirectoryConfig(directory));
        }
    }

}
