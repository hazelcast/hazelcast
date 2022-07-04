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

package com.hazelcast.spring;

import com.hazelcast.config.AbstractXmlConfigHelper;
import com.hazelcast.config.AliasedDiscoveryConfig;
import com.hazelcast.config.AutoDetectionConfig;
import com.hazelcast.config.ClassFilter;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.JavaSerializationFilterConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.config.PersistentMemoryConfig;
import com.hazelcast.config.PersistentMemoryDirectoryConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.internal.config.DomConfigHelper;
import com.hazelcast.internal.util.TriTuple;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.query.impl.IndexUtils;
import com.hazelcast.spring.config.ConfigFactory;
import com.hazelcast.spring.context.SpringManagedContext;
import com.hazelcast.spring.jet.JetBeanDefinitionParser;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanReference;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.support.ManagedSet;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static java.util.Arrays.asList;
import static org.springframework.util.Assert.isTrue;

/**
 * Base class of all Hazelcast BeanDefinitionParser implementations.
 * <p>
 * <ul>
 * <li>{@link HazelcastClientBeanDefinitionParser}</li>
 * <li>{@link HazelcastConfigBeanDefinitionParser}</li>
 * <li>{@link HazelcastInstanceDefinitionParser}</li>
 * <li>{@link HazelcastTypeBeanDefinitionParser}</li>
 * <li>{@link JetBeanDefinitionParser}</li>
 * </ul>
 */
@SuppressWarnings({"WeakerAccess", "checkstyle:methodcount"})
public abstract class AbstractHazelcastBeanDefinitionParser extends AbstractBeanDefinitionParser {

    /**
     * Base Helper class for Spring Xml Builder
     */
    public abstract static class SpringXmlBuilderHelper extends AbstractXmlConfigHelper {

        protected BeanDefinitionBuilder configBuilder;

        protected void handleCommonBeanAttributes(Node node, BeanDefinitionBuilder builder, ParserContext parserContext) {
            NamedNodeMap attributes = node.getAttributes();
            if (attributes != null) {
                Node lazyInitAttr = attributes.getNamedItem("lazy-init");
                if (lazyInitAttr != null) {
                    builder.setLazyInit(Boolean.valueOf(getTextContent(lazyInitAttr)));
                } else {
                    builder.setLazyInit(parserContext.isDefaultLazyInit());
                }

                if (parserContext.isNested()) {
                    BeanDefinition containingBeanDefinition = parserContext.getContainingBeanDefinition();
                    if (containingBeanDefinition != null) {
                        builder.setScope(containingBeanDefinition.getScope());
                    }
                } else {
                    Node scopeNode = attributes.getNamedItem("scope");
                    if (scopeNode != null) {
                        builder.setScope(getTextContent(scopeNode));
                    }
                }

                Node dependsOnNode = attributes.getNamedItem("depends-on");
                if (dependsOnNode != null) {
                    String[] dependsOn = getTextContent(dependsOnNode).split("[,;]");
                    for (String dep : dependsOn) {
                        builder.addDependsOn(dep.trim());
                    }
                }
            }
        }

        protected String getTextContent(Node node) {
            return DomConfigHelper.getTextContent(node, domLevel3);
        }

        protected String getAttribute(Node node, String attName) {
            return DomConfigHelper.getAttribute(node, attName, domLevel3);
        }

        protected BeanDefinitionBuilder createBeanBuilder(Class clazz) {
            BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(clazz);
            builder.setScope(configBuilder.getBeanDefinition().getScope());
            builder.setLazyInit(configBuilder.getBeanDefinition().isLazyInit());
            return builder;
        }

        protected BeanDefinitionBuilder createBeanBuilder(String className) {
            BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(className);
            builder.setScope(configBuilder.getBeanDefinition().getScope());
            builder.setLazyInit(configBuilder.getBeanDefinition().isLazyInit());
            return builder;
        }

        protected BeanDefinitionBuilder createAndFillBeanBuilder(Node node, Class clazz, String propertyName,
                                                                 BeanDefinitionBuilder parent, String... exceptPropertyNames) {
            BeanDefinitionBuilder builder = createBeanBuilder(clazz);
            AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();
            fillValues(node, builder, exceptPropertyNames);
            parent.addPropertyValue(propertyName, beanDefinition);
            return builder;
        }

        @SuppressWarnings("SameParameterValue")
        protected BeanDefinitionBuilder createAndFillListedBean(Node node, Class clazz, String propertyName,
                                                                ManagedMap<String, AbstractBeanDefinition> managedMap,
                                                                String... excludeNames) {

            BeanDefinitionBuilder builder = createBeanBuilder(clazz);
            AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();
            // "name"
            String name = getAttribute(node, propertyName);
            builder.addPropertyValue("name", name);
            fillValues(node, builder, excludeNames);
            managedMap.put(name, beanDefinition);
            return builder;
        }

        protected void fillValues(Node node, BeanDefinitionBuilder builder, String... excludeNames) {
            Collection<String> epn = excludeNames != null && excludeNames.length > 0
                    ? new HashSet<>(asList(excludeNames)) : null;
            fillAttributeValues(node, builder, epn);
            for (Node n : childElements(node)) {
                String name = xmlToJavaName(cleanNodeName(n));
                if (epn != null && epn.contains(name)) {
                    continue;
                }
                String value = getTextContent(n);
                builder.addPropertyValue(name, value);
            }
        }

        protected void fillAttributeValues(Node node, BeanDefinitionBuilder builder, String... excludeNames) {
            Collection<String> epn = excludeNames != null && excludeNames.length > 0
                    ? new HashSet<>(asList(excludeNames)) : null;
            fillAttributeValues(node, builder, epn);
        }

        protected void fillAttributeValues(Node node, BeanDefinitionBuilder builder, Collection<String> epn) {
            NamedNodeMap attributes = node.getAttributes();
            if (attributes != null) {
                for (int a = 0; a < attributes.getLength(); a++) {
                    Node attribute = attributes.item(a);
                    String name = xmlToJavaName(attribute.getNodeName());
                    if (epn != null && epn.contains(name)) {
                        continue;
                    }
                    String value = attribute.getNodeValue();
                    builder.addPropertyValue(name, value);
                }
            }
        }

        protected void fillAttributesForAliasedDiscoveryStrategy(AliasedDiscoveryConfig config, Node node,
                                                                 BeanDefinitionBuilder builder, String name) {
            NamedNodeMap attributes = node.getAttributes();
            if (attributes != null) {
                for (int i = 0; i < attributes.getLength(); i++) {
                    Node attribute = attributes.item(i);
                    config.setProperty(attribute.getNodeName(), attribute.getNodeValue());
                }
            }
            String propertyName = String.format("%sConfig", name);
            builder.addPropertyValue(propertyName, config);
        }

        protected ManagedList parseListeners(Node node, Class listenerConfigClass) {
            ManagedList<BeanDefinition> listeners = new ManagedList<>();
            String implementationAttr = "implementation";
            for (Node listenerNode : childElements(node)) {
                BeanDefinitionBuilder listenerConfBuilder = createBeanBuilder(listenerConfigClass);
                fillAttributeValues(listenerNode, listenerConfBuilder, implementationAttr);
                Node implementationNode = listenerNode.getAttributes().getNamedItem(implementationAttr);
                if (implementationNode != null) {
                    listenerConfBuilder.addPropertyReference(implementationAttr, getTextContent(implementationNode));
                }
                listeners.add(listenerConfBuilder.getBeanDefinition());
            }
            return listeners;
        }

        protected ManagedList parseProxyFactories(Node node, Class proxyFactoryConfigClass) {
            ManagedList<BeanDefinition> list = new ManagedList<>();
            for (Node instanceNode : childElements(node)) {
                BeanDefinitionBuilder confBuilder = createBeanBuilder(proxyFactoryConfigClass);
                fillAttributeValues(instanceNode, confBuilder);
                list.add(confBuilder.getBeanDefinition());
            }
            return list;
        }

        protected void handleDataSerializableFactories(Node node, BeanDefinitionBuilder serializationConfigBuilder) {
            ManagedMap<String, BeanReference> factories = new ManagedMap<>();
            ManagedMap<String, String> classNames = new ManagedMap<>();
            for (Node child : childElements(node)) {
                String name = cleanNodeName(child);
                if ("data-serializable-factory".equals(name)) {
                    NamedNodeMap attributes = child.getAttributes();
                    Node implRef = attributes.getNamedItem("implementation");
                    Node classNode = attributes.getNamedItem("class-name");
                    Node fidNode = attributes.getNamedItem("factory-id");
                    if (implRef != null) {
                        factories.put(getTextContent(fidNode), new RuntimeBeanReference(getTextContent(implRef)));
                    }
                    if (classNode != null) {
                        classNames.put(getTextContent(fidNode), getTextContent(classNode));
                    }
                }
            }
            serializationConfigBuilder.addPropertyValue("dataSerializableFactoryClasses", classNames);
            serializationConfigBuilder.addPropertyValue("dataSerializableFactories", factories);
        }

        protected void handleSerializers(Node node, BeanDefinitionBuilder serializationConfigBuilder) {
            BeanDefinitionBuilder globalSerializerConfigBuilder = null;
            String implementation = "implementation";
            String className = "class-name";
            String typeClassName = "type-class";
            String overrideJavaSerializationName = "override-java-serialization";

            ManagedList<BeanDefinition> serializers = new ManagedList<>();
            for (Node child : childElements(node)) {
                String name = cleanNodeName(child);
                if ("global-serializer".equals(name)) {
                    globalSerializerConfigBuilder = createGSConfigBuilder(GlobalSerializerConfig.class, child, implementation,
                            className, overrideJavaSerializationName);
                }
                if ("serializer".equals(name)) {
                    BeanDefinitionBuilder serializerConfigBuilder = createBeanBuilder(SerializerConfig.class);
                    fillAttributeValues(child, serializerConfigBuilder);
                    NamedNodeMap attributes = child.getAttributes();
                    Node implRef = attributes.getNamedItem(implementation);
                    Node classNode = attributes.getNamedItem(className);
                    Node typeClass = attributes.getNamedItem(typeClassName);

                    if (typeClass != null) {
                        serializerConfigBuilder.addPropertyValue("typeClassName", getTextContent(typeClass));
                    }
                    if (implRef != null) {
                        serializerConfigBuilder.addPropertyReference(xmlToJavaName(implementation), getTextContent(implRef));
                    }
                    if (classNode != null) {
                        serializerConfigBuilder.addPropertyValue(xmlToJavaName(className), getTextContent(classNode));
                    }
                    serializers.add(serializerConfigBuilder.getBeanDefinition());
                }
            }
            if (globalSerializerConfigBuilder != null) {
                serializationConfigBuilder.addPropertyValue("globalSerializerConfig",
                        globalSerializerConfigBuilder.getBeanDefinition());
            }
            serializationConfigBuilder.addPropertyValue("serializerConfigs", serializers);
        }

        private BeanDefinitionBuilder createGSConfigBuilder(Class<GlobalSerializerConfig> globalSerializerConfigClass, Node child,
                                                            String implementation, String className,
                                                            String overrideJavaSerializationName) {
            BeanDefinitionBuilder globalSerializerConfigBuilder = createBeanBuilder(globalSerializerConfigClass);
            NamedNodeMap attributes = child.getAttributes();
            Node implRef = attributes.getNamedItem(implementation);
            Node classNode = attributes.getNamedItem(className);
            Node overrideJavaSerializationNode = attributes.getNamedItem(overrideJavaSerializationName);
            if (implRef != null) {
                globalSerializerConfigBuilder.addPropertyReference(xmlToJavaName(implementation), getTextContent(implRef));
            }
            if (classNode != null) {
                globalSerializerConfigBuilder.addPropertyValue(xmlToJavaName(className), getTextContent(classNode));
            }
            if (overrideJavaSerializationNode != null) {
                String value = getTextContent(overrideJavaSerializationNode);
                globalSerializerConfigBuilder.addPropertyValue(xmlToJavaName(overrideJavaSerializationName), value);
            }
            return globalSerializerConfigBuilder;
        }

        protected void handlePortableFactories(Node node, BeanDefinitionBuilder serializationConfigBuilder) {
            ManagedMap<String, BeanReference> factories = new ManagedMap<>();
            ManagedMap<String, String> classNames = new ManagedMap<>();
            for (Node child : childElements(node)) {
                String name = cleanNodeName(child);
                if ("portable-factory".equals(name)) {
                    NamedNodeMap attributes = child.getAttributes();
                    Node implRef = attributes.getNamedItem("implementation");
                    Node classNode = attributes.getNamedItem("class-name");
                    Node fidNode = attributes.getNamedItem("factory-id");
                    if (implRef != null) {
                        factories.put(getTextContent(fidNode), new RuntimeBeanReference(getTextContent(implRef)));
                    }
                    if (classNode != null) {
                        classNames.put(getTextContent(fidNode), getTextContent(classNode));
                    }
                }
            }
            serializationConfigBuilder.addPropertyValue("portableFactoryClasses", classNames);
            serializationConfigBuilder.addPropertyValue("portableFactories", factories);
        }

        private void handleCompactSerialization(Node compactNode, BeanDefinitionBuilder serializationConfigBuilder) {
            BeanDefinitionBuilder compactSerializationConfigBuilder = createBeanBuilder(CompactSerializationConfig.class);
            compactSerializationConfigBuilder.getBeanDefinition().setBeanClass(ConfigFactory.class);
            compactSerializationConfigBuilder.setFactoryMethod("newCompactSerializationConfig");

            NamedNodeMap attributes = compactNode.getAttributes();
            Node enabledNode = attributes.getNamedItem("enabled");
            if (enabledNode != null) {
                String value = getTextContent(enabledNode);
                compactSerializationConfigBuilder.addConstructorArgValue(value);
            }

            ManagedMap<String, TriTuple<String, String, String>> registrations = new ManagedMap<>();
            for (Node child : childElements(compactNode)) {
                String name = cleanNodeName(child);
                if ("registered-classes".equals(name)) {
                    handleRegisteredClasses(child, registrations);
                }
            }
            compactSerializationConfigBuilder.addConstructorArgValue(registrations);

            BeanDefinition compactBeanDefinition = compactSerializationConfigBuilder.getBeanDefinition();
            serializationConfigBuilder.addPropertyValue("compactSerializationConfig", compactBeanDefinition);
        }

        private void handleRegisteredClasses(Node registeredClasses,
                                             Map<String, TriTuple<String, String, String>> registrations) {
            for (Node node : childElements(registeredClasses)) {
                String nodeName = cleanNodeName(node);
                if (!"class".equals(nodeName)) {
                    continue;
                }

                String className = getTextContent(node);
                NamedNodeMap classAttributes = node.getAttributes();
                Node typeNameNode = classAttributes.getNamedItem("type-name");
                Node serializerNode = classAttributes.getNamedItem("serializer");
                if (typeNameNode != null ^ serializerNode != null) {
                    throw new InvalidConfigurationException("Either both 'type-name' and 'serializer' attributes "
                            + "must be defined to register a class with an explicit serializer, "
                            + "or no attributes should be defined to register a class to be used with "
                            + "reflective compact serializer.");
                }

                String typeName = typeNameNode != null ? getTextContent(typeNameNode) : className;
                String serializerName = serializerNode != null ? getTextContent(serializerNode) : null;

                TriTuple<String, String, String> registration = TriTuple.of(className, typeName, serializerName);
                if (registrations.put(typeName, registration) != null) {
                    throw new InvalidConfigurationException("Found a duplicate type name registration for " + typeName);
                }
            }
        }

        protected void handleSerialization(Node node) {
            BeanDefinitionBuilder serializationConfigBuilder = createBeanBuilder(SerializationConfig.class);
            AbstractBeanDefinition beanDefinition = serializationConfigBuilder.getBeanDefinition();
            fillAttributeValues(node, serializationConfigBuilder);
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("data-serializable-factories".equals(nodeName)) {
                    handleDataSerializableFactories(child, serializationConfigBuilder);
                } else if ("portable-factories".equals(nodeName)) {
                    handlePortableFactories(child, serializationConfigBuilder);
                } else if ("serializers".equals(nodeName)) {
                    handleSerializers(child, serializationConfigBuilder);
                } else if ("java-serialization-filter".equals(nodeName)) {
                    handleJavaSerializationFilter(child, serializationConfigBuilder);
                } else if ("compact-serialization".equals(nodeName)) {
                    handleCompactSerialization(child, serializationConfigBuilder);
                }
            }
            configBuilder.addPropertyValue("serializationConfig", beanDefinition);
        }

        protected void handleSocketInterceptorConfig(Node node, BeanDefinitionBuilder networkConfigBuilder) {
            BeanDefinitionBuilder socketInterceptorConfigBuilder = createBeanBuilder(SocketInterceptorConfig.class);
            String implAttribute = "implementation";
            fillAttributeValues(node, socketInterceptorConfigBuilder, implAttribute);
            Node implNode = node.getAttributes().getNamedItem(implAttribute);
            String implementation = implNode != null ? getTextContent(implNode) : null;
            if (implementation != null) {
                socketInterceptorConfigBuilder.addPropertyReference(xmlToJavaName(implAttribute), implementation);
            }
            for (Node child : childElements(node)) {
                String name = cleanNodeName(child);
                if ("properties".equals(name)) {
                    handleProperties(child, socketInterceptorConfigBuilder);
                }
            }
            networkConfigBuilder.addPropertyValue("socketInterceptorConfig",
                    socketInterceptorConfigBuilder.getBeanDefinition());
        }

        void handleClusterAttributes(Node node) {
            NamedNodeMap attributes = node.getAttributes();
            if (attributes != null) {
                for (int a = 0; a < attributes.getLength(); a++) {
                    Node att = attributes.item(a);
                    String name = att.getNodeName();
                    String value = att.getNodeValue();
                    if ("name".equals(name)) {
                        configBuilder.addPropertyValue("clusterName", value);
                    } else if ("password".equals(name)) {
                        configBuilder.addPropertyValue("clusterPassword", value);
                    }
                }
            }
        }

        protected void handleProperties(Node node, BeanDefinitionBuilder beanDefinitionBuilder) {
            ManagedMap properties = parseProperties(node);
            beanDefinitionBuilder.addPropertyValue("properties", properties);
        }

        protected ManagedMap parseProperties(Node node) {
            ManagedMap<String, String> properties = new ManagedMap<>();
            for (Node n : childElements(node)) {
                String name = cleanNodeName(n);
                String propertyName;
                if (!"property".equals(name)) {
                    continue;
                }
                propertyName = getTextContent(n.getAttributes().getNamedItem("name")).trim();
                String value = getTextContent(n);
                properties.put(propertyName, value);
            }
            return properties;
        }

        protected void handleSpringAware() {
            BeanDefinitionBuilder managedContextBeanBuilder = createBeanBuilder(SpringManagedContext.class);
            configBuilder.addPropertyValue("managedContext", managedContextBeanBuilder.getBeanDefinition());
        }

        protected BeanDefinition getEvictionConfig(Node node, boolean isNearCache, boolean isIMap) {
            Node size = node.getAttributes().getNamedItem("size");
            Node maxSizePolicy = node.getAttributes().getNamedItem("max-size-policy");
            Node evictionPolicy = node.getAttributes().getNamedItem("eviction-policy");
            Node comparatorClassName = node.getAttributes().getNamedItem("comparator-class-name");
            Node comparatorBean = node.getAttributes().getNamedItem("comparator-bean");
            if (comparatorClassName != null && comparatorBean != null) {
                throw new InvalidConfigurationException("Only one of the `comparator-class-name` and `comparator-bean`"
                        + " attributes can be configured inside eviction configuration!");
            }

            BeanDefinitionBuilder evictionConfigBuilder = createBeanBuilder(EvictionConfig.class);
            evictionConfigBuilder.getBeanDefinition().setBeanClass(ConfigFactory.class);
            evictionConfigBuilder.setFactoryMethod("newEvictionConfig");

            String comparatorClassNameValue = null;

            if (comparatorClassName != null) {
                comparatorClassNameValue = getTextContent(comparatorClassName);
            }

            evictionConfigBuilder.addConstructorArgValue(getTextContent(size));
            evictionConfigBuilder.addConstructorArgValue(getTextContent(maxSizePolicy));
            evictionConfigBuilder.addConstructorArgValue(getTextContent(evictionPolicy));
            evictionConfigBuilder.addConstructorArgValue(isNearCache);
            evictionConfigBuilder.addConstructorArgValue(isIMap);
            evictionConfigBuilder.addConstructorArgValue(comparatorClassNameValue);
            if (comparatorBean == null) {
                evictionConfigBuilder.addConstructorArgValue(null);
            } else {
                evictionConfigBuilder.addConstructorArgReference(getTextContent(comparatorBean));
            }
            return evictionConfigBuilder.getBeanDefinition();
        }

        protected BeanDefinition getPreloaderConfig(Node node) {
            Node enabled = node.getAttributes().getNamedItem("enabled");
            Node directory = node.getAttributes().getNamedItem("directory");
            Node storeInitialDelaySeconds = node.getAttributes().getNamedItem("store-initial-delay-seconds");
            Node storeIntervalSeconds = node.getAttributes().getNamedItem("store-interval-seconds");

            BeanDefinitionBuilder nearCachePreloaderConfigBuilder = createBeanBuilder(NearCachePreloaderConfig.class);

            String storeInitialDelaySecondsValue = String.valueOf(NearCachePreloaderConfig.DEFAULT_STORE_INITIAL_DELAY_SECONDS);
            String storeIntervalSecondsValue = String.valueOf(NearCachePreloaderConfig.DEFAULT_STORE_INTERVAL_SECONDS);

            if (storeInitialDelaySeconds != null) {
                storeInitialDelaySecondsValue = getTextContent(storeInitialDelaySeconds);
            }
            if (storeIntervalSeconds != null) {
                storeIntervalSecondsValue = getTextContent(storeIntervalSeconds);
            }

            nearCachePreloaderConfigBuilder.addPropertyValue("enabled", getTextContent(enabled));
            nearCachePreloaderConfigBuilder.addPropertyValue("directory", getTextContent(directory));
            nearCachePreloaderConfigBuilder.addPropertyValue("storeInitialDelaySeconds", storeInitialDelaySecondsValue);
            nearCachePreloaderConfigBuilder.addPropertyValue("storeIntervalSeconds", storeIntervalSecondsValue);

            return nearCachePreloaderConfigBuilder.getBeanDefinition();
        }

        protected void handleNativeMemory(Node node) {
            BeanDefinitionBuilder nativeMemoryConfigBuilder = createBeanBuilder(NativeMemoryConfig.class);
            AbstractBeanDefinition beanDefinition = nativeMemoryConfigBuilder.getBeanDefinition();
            fillAttributeValues(node, nativeMemoryConfigBuilder, "persistentMemoryDirectory");
            ManagedList<BeanDefinition> directories = new ManagedList<>();
            BeanDefinitionBuilder pmemConfigBuilder = createBeanBuilder(PersistentMemoryConfig.class);
            for (Node child : childElements(node)) {
                String nodeName = cleanNodeName(child);
                if ("size".equals(nodeName)) {
                    handleMemorySizeConfig(child, nativeMemoryConfigBuilder);
                } else if ("persistent-memory".equals(nodeName)) {
                    handlePersistentMemoryConfig(child, pmemConfigBuilder, directories);
                }
            }

            Node attrPmemDirectory = node.getAttributes().getNamedItem("persistent-memory-directory");
            if (attrPmemDirectory != null) {
                BeanDefinitionBuilder pmemDirConfigBuilder = createBeanBuilder(PersistentMemoryDirectoryConfig.class);
                pmemDirConfigBuilder.addConstructorArgValue(getTextContent(attrPmemDirectory));
                directories.add(pmemDirConfigBuilder.getBeanDefinition());
            }

            if (!directories.isEmpty()) {
                pmemConfigBuilder.addPropertyValue("directoryConfigs", directories);
            }

            nativeMemoryConfigBuilder.addPropertyValue("persistentMemoryConfig", pmemConfigBuilder.getBeanDefinition());
            configBuilder.addPropertyValue("nativeMemoryConfig", beanDefinition);
        }

        private void handlePersistentMemoryConfig(Node pmemNode, BeanDefinitionBuilder pmemConfigBuilder,
                                                  ManagedList<BeanDefinition> directoriesList) {
            Node enabledNode = pmemNode.getAttributes().getNamedItem("enabled");
            pmemConfigBuilder.addPropertyValue("enabled", getTextContent(enabledNode));

            Node mode = pmemNode.getAttributes().getNamedItem("mode");
            pmemConfigBuilder.addPropertyValue("mode", getTextContent(mode));


            for (Node dirsNode : childElements(pmemNode)) {
                String dirsNodeName = cleanNodeName(dirsNode);
                if ("directories".equals(dirsNodeName)) {
                    for (Node dirNode : childElements(dirsNode)) {
                        String dirNodeName = cleanNodeName(dirNode);
                        if ("directory".equals(dirNodeName)) {
                            BeanDefinitionBuilder pmemDirConfigBuilder = createBeanBuilder(PersistentMemoryDirectoryConfig.class);
                            NamedNodeMap attributes = dirNode.getAttributes();
                            Node numaNode = attributes.getNamedItem("numa-node");
                            pmemDirConfigBuilder.addConstructorArgValue(getTextContent(dirNode));
                            pmemDirConfigBuilder.addConstructorArgValue(getTextContent(numaNode));
                            directoriesList.add(pmemDirConfigBuilder.getBeanDefinition());
                        }
                    }
                }
            }
        }

        private void handleMemorySizeConfig(Node node, BeanDefinitionBuilder nativeMemoryConfigBuilder) {
            BeanDefinitionBuilder memorySizeConfigBuilder = createBeanBuilder(MemorySize.class);
            NamedNodeMap attributes = node.getAttributes();
            Node value = attributes.getNamedItem("value");
            Node unit = attributes.getNamedItem("unit");
            memorySizeConfigBuilder.addConstructorArgValue(getTextContent(value));
            memorySizeConfigBuilder.addConstructorArgValue(getTextContent(unit));
            nativeMemoryConfigBuilder.addPropertyValue("size", memorySizeConfigBuilder.getBeanDefinition());
        }

        protected void handleDiscoveryStrategies(Node node, BeanDefinitionBuilder joinConfigBuilder) {
            BeanDefinitionBuilder discoveryConfigBuilder = createBeanBuilder(DiscoveryConfig.class);
            ManagedList<BeanDefinition> discoveryStrategyConfigs = new ManagedList<>();
            for (Node child : childElements(node)) {
                String name = cleanNodeName(child);
                if ("discovery-strategy".equals(name)) {
                    handleDiscoveryStrategy(child, discoveryStrategyConfigs);
                } else if ("node-filter".equals(name)) {
                    handleDiscoveryNodeFilter(child, discoveryConfigBuilder);
                } else if ("discovery-service-provider".equals(name)) {
                    handleDiscoveryServiceProvider(child, discoveryConfigBuilder);
                }
            }
            discoveryConfigBuilder.addPropertyValue("discoveryStrategyConfigs", discoveryStrategyConfigs);
            joinConfigBuilder.addPropertyValue("discoveryConfig", discoveryConfigBuilder.getBeanDefinition());
        }

        protected void handleAutoDetection(Node node, BeanDefinitionBuilder joinConfigBuilder) {
            BeanDefinitionBuilder autoDetectionConfigBuilder = createBeanBuilder(AutoDetectionConfig.class);
            autoDetectionConfigBuilder.addPropertyValue("enabled", getAttribute(node, "enabled"));
            joinConfigBuilder.addPropertyValue("autoDetectionConfig", autoDetectionConfigBuilder.getBeanDefinition());
        }

        protected void handleIndex(ManagedList<BeanDefinition> indexes, Node indexNode) {
            BeanDefinitionBuilder indexConfBuilder = createBeanBuilder(IndexConfig.class);

            NamedNodeMap attributes = indexNode.getAttributes();

            // Resolve name.
            String name = getTextContent(attributes.getNamedItem("name"));
            indexConfBuilder.addPropertyValue("name", name.isEmpty() ? null : name);

            // Resolve type.
            String typeStr = getTextContent(attributes.getNamedItem("type"));
            IndexType type = IndexUtils.getIndexTypeFromXmlName(typeStr);
            indexConfBuilder.addPropertyValue("type", type);

            // Resolve columns.
            List<String> columns = new ArrayList<>();

            for (Node columnsNode : childElements(indexNode)) {
                if ("attributes".equals(cleanNodeName(columnsNode))) {
                    for (Node columnNode : childElements(columnsNode)) {
                        if ("attribute".equals(cleanNodeName(columnNode))) {
                            columns.add(getTextContent(columnNode));
                        }
                    }
                }
            }

            indexConfBuilder.addPropertyValue("attributes", columns);

            indexes.add(indexConfBuilder.getBeanDefinition());
        }

        private void handleDiscoveryServiceProvider(Node node, BeanDefinitionBuilder discoveryConfigBuilder) {
            NamedNodeMap attributes = node.getAttributes();
            Node implNode = attributes.getNamedItem("implementation");
            String implementation = getTextContent(implNode).trim();
            isTrue(!implementation.isEmpty(), "'implementation' attribute is required to create DiscoveryServiceProvider!");
            discoveryConfigBuilder.addPropertyReference("discoveryServiceProvider", implementation);
        }

        private void handleDiscoveryNodeFilter(Node node, BeanDefinitionBuilder discoveryConfigBuilder) {
            NamedNodeMap attributes = node.getAttributes();
            Node classNameNode = attributes.getNamedItem("class-name");
            String className = getTextContent(classNameNode).trim();
            Node implNode = attributes.getNamedItem("implementation");
            String implementation = getTextContent(implNode).trim();
            isTrue(!className.isEmpty() || !implementation.isEmpty(),
                    "One of 'class-name' or 'implementation' attributes is required to create NodeFilter!");
            if (!className.isEmpty()) {
                discoveryConfigBuilder.addPropertyValue("nodeFilterClass", className);
            }
            if (!implementation.isEmpty()) {
                discoveryConfigBuilder.addPropertyReference("nodeFilter", implementation);
            }
        }

        private void handleDiscoveryStrategy(Node node, ManagedList<BeanDefinition> discoveryStrategyConfigs) {
            BeanDefinitionBuilder discoveryStrategyConfigBuilder = createBeanBuilder(DiscoveryStrategyConfig.class);
            NamedNodeMap attributes = node.getAttributes();
            Node classNameNode = attributes.getNamedItem("class-name");
            String className = getTextContent(classNameNode).trim();
            Node implNode = attributes.getNamedItem("discovery-strategy-factory");
            String implementation = getTextContent(implNode).trim();
            isTrue(!className.isEmpty() || !implementation.isEmpty(),
                    "One of 'class-name' or 'implementation' attributes is required to create DiscoveryStrategyConfig!");
            if (!implementation.isEmpty()) {
                discoveryStrategyConfigBuilder.addConstructorArgReference(implementation);
            } else {
                discoveryStrategyConfigBuilder.addConstructorArgValue(className);
            }

            for (Node child : childElements(node)) {
                String name = cleanNodeName(child);
                if ("properties".equals(name)) {
                    ManagedMap properties = parseProperties(child);
                    if (!properties.isEmpty()) {
                        discoveryStrategyConfigBuilder.addConstructorArgValue(properties);
                    }
                }
            }
            discoveryStrategyConfigs.add(discoveryStrategyConfigBuilder.getBeanDefinition());
        }

        protected void handleJavaSerializationFilter(final Node node, BeanDefinitionBuilder serializationConfigBuilder) {
            BeanDefinitionBuilder filterConfigBuilder = createBeanBuilder(JavaSerializationFilterConfig.class);
            for (Node child : childElements(node)) {
                String name = cleanNodeName(child);
                if ("blacklist".equals(name)) {
                    filterConfigBuilder.addPropertyValue("blacklist", createFilterListBean(child));
                } else if ("whitelist".equals(name)) {
                    filterConfigBuilder.addPropertyValue("whitelist", createFilterListBean(child));
                }
            }
            Node defaultsDisabledAttr = node.getAttributes().getNamedItem("defaults-disabled");
            filterConfigBuilder.addPropertyValue("defaultsDisabled", getTextContent(defaultsDisabledAttr));
            serializationConfigBuilder.addPropertyValue("javaSerializationFilterConfig",
                    filterConfigBuilder.getBeanDefinition());
        }

        private AbstractBeanDefinition createFilterListBean(Node node) {
            BeanDefinitionBuilder filterListBuilder = createBeanBuilder(ClassFilter.class);
            ManagedSet<String> classes = new ManagedSet<>();
            ManagedSet<String> packages = new ManagedSet<>();
            ManagedSet<String> prefixes = new ManagedSet<>();
            for (Node child : childElements(node)) {
                String name = cleanNodeName(child);
                if ("class".equals(name)) {
                    classes.add(getTextContent(child));
                } else if ("package".equals(name)) {
                    packages.add(getTextContent(child));
                } else if ("prefix".equals(name)) {
                    prefixes.add(getTextContent(child));
                }
            }
            filterListBuilder.addPropertyValue("classes", classes);
            filterListBuilder.addPropertyValue("packages", packages);
            filterListBuilder.addPropertyValue("prefixes", prefixes);
            return filterListBuilder.getBeanDefinition();
        }
    }

}
