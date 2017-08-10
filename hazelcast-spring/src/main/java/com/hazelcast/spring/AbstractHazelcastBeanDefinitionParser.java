/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.spring.context.SpringManagedContext;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.Assert;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import static com.hazelcast.internal.config.ConfigValidator.checkEvictionConfig;
import static com.hazelcast.util.StringUtil.upperCaseInternal;

/**
 * Base class of all Hazelcast BeanDefinitionParser implementations.
 * <p/>
 * <ul>
 * <li>{@link HazelcastClientBeanDefinitionParser}</li>
 * <li>{@link HazelcastConfigBeanDefinitionParser}</li>
 * <li>{@link HazelcastInstanceDefinitionParser}</li>
 * <li>{@link HazelcastTypeBeanDefinitionParser}</li>
 * </ul>
 */
public abstract class AbstractHazelcastBeanDefinitionParser extends AbstractBeanDefinitionParser {

    /**
     * Base Helper class for Spring Xml Builder
     */
    public abstract class SpringXmlBuilderHelper extends AbstractXmlConfigHelper {

        protected BeanDefinitionBuilder configBuilder;

        protected void handleCommonBeanAttributes(Node node, BeanDefinitionBuilder builder, ParserContext parserContext) {
            final NamedNodeMap attributes = node.getAttributes();
            if (attributes != null) {
                Node lazyInitAttr = attributes.getNamedItem("lazy-init");
                if (lazyInitAttr != null) {
                    builder.setLazyInit(Boolean.valueOf(getTextContent(lazyInitAttr)));
                } else {
                    builder.setLazyInit(parserContext.isDefaultLazyInit());
                }

                if (parserContext.isNested()) {
                    builder.setScope(parserContext.getContainingBeanDefinition().getScope());
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

        protected BeanDefinitionBuilder createBeanBuilder(final Class clazz) {
            BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(clazz);
            builder.setScope(configBuilder.getBeanDefinition().getScope());
            builder.setLazyInit(configBuilder.getBeanDefinition().isLazyInit());
            return builder;
        }

        protected BeanDefinitionBuilder createAndFillBeanBuilder(Node node, final Class clazz,
                                                                 final String propertyName,
                                                                 final BeanDefinitionBuilder parent,
                                                                 final String... exceptPropertyNames) {
            BeanDefinitionBuilder builder = createBeanBuilder(clazz);
            final AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();
            fillValues(node, builder, exceptPropertyNames);
            parent.addPropertyValue(propertyName, beanDefinition);
            return builder;
        }

        protected void createAndFillListedBean(final Node node,
                                               final Class clazz,
                                               final String propertyName,
                                               final ManagedMap<String, AbstractBeanDefinition> managedMap,
                                               final String... excludeNames) {
            final BeanDefinitionBuilder builder = createBeanBuilder(clazz);
            final AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();
            //"name"
            final String name = getAttribute(node, propertyName);
            builder.addPropertyValue("name", name);
            fillValues(node, builder, excludeNames);
            managedMap.put(name, beanDefinition);
        }

        protected void fillValues(Node node, BeanDefinitionBuilder builder, String... excludeNames) {
            Collection<String> epn = excludeNames != null && excludeNames.length > 0
                    ? new HashSet<String>(Arrays.asList(excludeNames)) : null;
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
                    ? new HashSet<String>(Arrays.asList(excludeNames)) : null;
            fillAttributeValues(node, builder, epn);
        }

        protected void fillAttributeValues(Node node, BeanDefinitionBuilder builder, Collection<String> epn) {
            final NamedNodeMap atts = node.getAttributes();
            if (atts != null) {
                for (int a = 0; a < atts.getLength(); a++) {
                    final Node att = atts.item(a);
                    final String name = xmlToJavaName(att.getNodeName());
                    if (epn != null && epn.contains(name)) {
                        continue;
                    }
                    final String value = att.getNodeValue();
                    builder.addPropertyValue(name, value);
                }
            }
        }

        protected ManagedList parseListeners(Node node, Class listenerConfigClass) {
            ManagedList listeners = new ManagedList();
            final String implementationAttr = "implementation";
            for (Node listenerNode : childElements(node)) {
                final BeanDefinitionBuilder listenerConfBuilder = createBeanBuilder(listenerConfigClass);
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
            ManagedList list = new ManagedList();
            for (Node instanceNode : childElements(node)) {
                final BeanDefinitionBuilder confBuilder = createBeanBuilder(proxyFactoryConfigClass);
                fillAttributeValues(instanceNode, confBuilder);
                list.add(confBuilder.getBeanDefinition());
            }
            return list;
        }


        protected void handleDataSerializableFactories(final Node node, final BeanDefinitionBuilder serializationConfigBuilder) {
            ManagedMap factories = new ManagedMap();
            ManagedMap<Integer, String> classNames = new ManagedMap<Integer, String>();
            for (Node child : childElements(node)) {
                final String name = cleanNodeName(child);
                if ("data-serializable-factory".equals(name)) {
                    final NamedNodeMap attrs = child.getAttributes();
                    final Node implRef = attrs.getNamedItem("implementation");
                    final Node classNode = attrs.getNamedItem("class-name");
                    final Node fidNode = attrs.getNamedItem("factory-id");
                    if (implRef != null) {
                        factories.put(Integer.parseInt(getTextContent(fidNode))
                                , new RuntimeBeanReference(getTextContent(implRef)));
                    }
                    if (classNode != null) {
                        classNames.put(Integer.parseInt(getTextContent(fidNode)), getTextContent(classNode));
                    }
                }
            }
            serializationConfigBuilder.addPropertyValue("dataSerializableFactoryClasses", classNames);
            serializationConfigBuilder.addPropertyValue("dataSerializableFactories", factories);
        }

        protected void handleSerializers(final Node node, final BeanDefinitionBuilder serializationConfigBuilder) {
            BeanDefinitionBuilder globalSerializerConfigBuilder = null;
            String implementation = "implementation";
            String className = "class-name";
            String typeClassName = "type-class";
            String overrideJavaSerializationName = "override-java-serialization";

            ManagedList serializers = new ManagedList();
            for (Node child : childElements(node)) {
                final String name = cleanNodeName(child);
                if ("global-serializer".equals(name)) {
                    globalSerializerConfigBuilder = createGSConfigBuilder(GlobalSerializerConfig.class, child, implementation,
                            className, overrideJavaSerializationName);
                }
                if ("serializer".equals(name)) {
                    BeanDefinitionBuilder serializerConfigBuilder = createBeanBuilder(SerializerConfig.class);
                    fillAttributeValues(child, serializerConfigBuilder);
                    final NamedNodeMap attrs = child.getAttributes();
                    final Node implRef = attrs.getNamedItem(implementation);
                    final Node classNode = attrs.getNamedItem(className);

                    final Node typeClass = attrs.getNamedItem(typeClassName);

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
                serializationConfigBuilder.addPropertyValue("globalSerializerConfig"
                        , globalSerializerConfigBuilder.getBeanDefinition());
            }
            serializationConfigBuilder.addPropertyValue("serializerConfigs", serializers);
        }

        private BeanDefinitionBuilder createGSConfigBuilder(Class<GlobalSerializerConfig> globalSerializerConfigClass, Node child,
                                                            String implementation, String className,
                                                            String overrideJavaSerializationName) {
            BeanDefinitionBuilder globalSerializerConfigBuilder = createBeanBuilder(globalSerializerConfigClass);
            final NamedNodeMap attrs = child.getAttributes();
            final Node implRef = attrs.getNamedItem(implementation);
            final Node classNode = attrs.getNamedItem(className);
            final Node overrideJavaSerializationNode = attrs.getNamedItem(overrideJavaSerializationName);
            if (implRef != null) {
                globalSerializerConfigBuilder.addPropertyReference(xmlToJavaName(implementation), getTextContent(implRef));
            }
            if (classNode != null) {
                globalSerializerConfigBuilder.addPropertyValue(xmlToJavaName(className), getTextContent(classNode));
            }
            if (overrideJavaSerializationNode != null) {
                boolean value = getBooleanValue(getTextContent(overrideJavaSerializationNode));
                globalSerializerConfigBuilder.addPropertyValue(xmlToJavaName(overrideJavaSerializationName), value);
            }
            return globalSerializerConfigBuilder;
        }

        protected void handlePortableFactories(final Node node, final BeanDefinitionBuilder serializationConfigBuilder) {
            ManagedMap factories = new ManagedMap();
            ManagedMap<Integer, String> classNames = new ManagedMap<Integer, String>();
            for (Node child : childElements(node)) {
                final String name = cleanNodeName(child);
                if ("portable-factory".equals(name)) {
                    final NamedNodeMap attrs = child.getAttributes();
                    final Node implRef = attrs.getNamedItem("implementation");
                    final Node classNode = attrs.getNamedItem("class-name");
                    final Node fidNode = attrs.getNamedItem("factory-id");
                    if (implRef != null) {
                        factories.put(Integer.parseInt(getTextContent(fidNode))
                                , new RuntimeBeanReference(getTextContent(implRef)));
                    }
                    if (classNode != null) {
                        classNames.put(Integer.parseInt(getTextContent(fidNode)), getTextContent(classNode));
                    }
                }
            }
            serializationConfigBuilder.addPropertyValue("portableFactoryClasses", classNames);
            serializationConfigBuilder.addPropertyValue("portableFactories", factories);
        }

        protected void handleSerialization(final Node node) {
            final BeanDefinitionBuilder serializationConfigBuilder = createBeanBuilder(SerializationConfig.class);
            final AbstractBeanDefinition beanDefinition = serializationConfigBuilder.getBeanDefinition();
            fillAttributeValues(node, serializationConfigBuilder);
            for (Node child : childElements(node)) {
                final String nodeName = cleanNodeName(child);
                if ("data-serializable-factories".equals(nodeName)) {
                    handleDataSerializableFactories(child, serializationConfigBuilder);
                } else if ("portable-factories".equals(nodeName)) {
                    handlePortableFactories(child, serializationConfigBuilder);
                } else if ("serializers".equals(nodeName)) {
                    handleSerializers(child, serializationConfigBuilder);
                }
            }
            configBuilder.addPropertyValue("serializationConfig", beanDefinition);
        }

        protected void handleSocketInterceptorConfig(final Node node, final BeanDefinitionBuilder networkConfigBuilder) {
            BeanDefinitionBuilder socketInterceptorConfigBuilder = createBeanBuilder(SocketInterceptorConfig.class);
            final String implAttribute = "implementation";
            fillAttributeValues(node, socketInterceptorConfigBuilder, implAttribute);
            Node implNode = node.getAttributes().getNamedItem(implAttribute);
            String implementation = implNode != null ? getTextContent(implNode) : null;
            if (implementation != null) {
                socketInterceptorConfigBuilder.addPropertyReference(xmlToJavaName(implAttribute), implementation);
            }
            for (Node child : childElements(node)) {
                final String name = cleanNodeName(child);
                if ("properties".equals(name)) {
                    handleProperties(child, socketInterceptorConfigBuilder);
                }
            }
            networkConfigBuilder.addPropertyValue("socketInterceptorConfig",
                    socketInterceptorConfigBuilder.getBeanDefinition());
        }

        protected void handleProperties(final Node node, BeanDefinitionBuilder beanDefinitionBuilder) {
            ManagedMap properties = parseProperties(node);
            beanDefinitionBuilder.addPropertyValue("properties", properties);
        }

        protected ManagedMap parseProperties(final Node node) {
            ManagedMap properties = new ManagedMap();
            for (Node n : childElements(node)) {
                final String name = cleanNodeName(n);
                final String propertyName;
                if (!"property".equals(name)) {
                    continue;
                }
                propertyName = getTextContent(n.getAttributes().getNamedItem("name")).trim();
                final String value = getTextContent(n);
                properties.put(propertyName, value);
            }
            return properties;
        }

        protected void handleSpringAware() {
            BeanDefinitionBuilder managedContextBeanBuilder = createBeanBuilder(SpringManagedContext.class);
            configBuilder.addPropertyValue("managedContext", managedContextBeanBuilder.getBeanDefinition());
        }

        @SuppressWarnings("checkstyle:npathcomplexity")
        protected BeanDefinition getEvictionConfig(final Node node) {
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

            Integer sizeValue = EvictionConfig.DEFAULT_MAX_ENTRY_COUNT;
            EvictionConfig.MaxSizePolicy maxSizePolicyValue = EvictionConfig.DEFAULT_MAX_SIZE_POLICY;
            EvictionPolicy evictionPolicyValue = EvictionConfig.DEFAULT_EVICTION_POLICY;
            String comparatorClassNameValue = null;
            String comparatorBeanValue = null;

            if (size != null) {
                sizeValue = Integer.parseInt(getTextContent(size));
            }
            if (maxSizePolicy != null) {
                maxSizePolicyValue =  EvictionConfig.MaxSizePolicy.valueOf(
                        upperCaseInternal(getTextContent(maxSizePolicy)));
            }
            if (evictionPolicy != null) {
                evictionPolicyValue = EvictionPolicy.valueOf(
                        upperCaseInternal(getTextContent(evictionPolicy)));
            }
            if (comparatorClassName != null) {
                comparatorClassNameValue = getTextContent(comparatorClassName);
            }
            if (comparatorBean != null) {
                comparatorBeanValue = getTextContent(comparatorBean);
            }

            try {
                checkEvictionConfig(evictionPolicyValue, comparatorClassNameValue, comparatorBean, false);
            } catch (IllegalArgumentException e) {
                throw new InvalidConfigurationException(e.getMessage());
            }

            evictionConfigBuilder.addPropertyValue("size", sizeValue);
            evictionConfigBuilder.addPropertyValue("maximumSizePolicy", maxSizePolicyValue);
            evictionConfigBuilder.addPropertyValue("evictionPolicy", evictionPolicyValue);
            if (comparatorClassNameValue != null) {
                evictionConfigBuilder.addPropertyValue("comparatorClassName", comparatorClassNameValue);
            }
            if (comparatorBean != null) {
                evictionConfigBuilder.addPropertyReference("comparator", comparatorBeanValue);
            }

            return evictionConfigBuilder.getBeanDefinition();
        }

        protected BeanDefinition getPreloaderConfig(final Node node) {
            Node enabled = node.getAttributes().getNamedItem("enabled");
            Node directory = node.getAttributes().getNamedItem("directory");
            Node storeInitialDelaySeconds = node.getAttributes().getNamedItem("store-initial-delay-seconds");
            Node storeIntervalSeconds = node.getAttributes().getNamedItem("store-interval-seconds");

            BeanDefinitionBuilder nearCachePreloaderConfigBuilder = createBeanBuilder(NearCachePreloaderConfig.class);

            Boolean enabledValue = Boolean.FALSE;
            String directoryValue = "";
            Integer storeInitialDelaySecondsValue = NearCachePreloaderConfig.DEFAULT_STORE_INITIAL_DELAY_SECONDS;
            Integer storeIntervalSecondsValue = NearCachePreloaderConfig.DEFAULT_STORE_INTERVAL_SECONDS;

            if (enabled != null) {
                enabledValue = Boolean.parseBoolean(getTextContent(enabled));
            }
            if (directory != null) {
                directoryValue =  getTextContent(directory);
            }
            if (storeInitialDelaySeconds != null) {
                storeInitialDelaySecondsValue = Integer.parseInt(getTextContent(storeInitialDelaySeconds));
            }
            if (storeIntervalSeconds != null) {
                storeIntervalSecondsValue = Integer.parseInt(getTextContent(storeIntervalSeconds));
            }

            nearCachePreloaderConfigBuilder.addPropertyValue("enabled", enabledValue);
            nearCachePreloaderConfigBuilder.addPropertyValue("directory", directoryValue);
            nearCachePreloaderConfigBuilder.addPropertyValue("storeInitialDelaySeconds", storeInitialDelaySecondsValue);
            nearCachePreloaderConfigBuilder.addPropertyValue("storeIntervalSeconds", storeIntervalSecondsValue);

            return nearCachePreloaderConfigBuilder.getBeanDefinition();
        }

        protected void handleDiscoveryStrategies(Node node, BeanDefinitionBuilder joinConfigBuilder) {
            final BeanDefinitionBuilder discoveryConfigBuilder =
                    createBeanBuilder(DiscoveryConfig.class);
            final ManagedList discoveryStrategyConfigs = new ManagedList();
            for (Node child : childElements(node)) {
                final String name = cleanNodeName(child);
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

        private void handleDiscoveryServiceProvider(Node node, BeanDefinitionBuilder discoveyConfigBuilder) {
            final NamedNodeMap attrs = node.getAttributes();
            Node implNode = attrs.getNamedItem("implementation");
            String implementation = implNode != null ? getTextContent(implNode) : null;
            Assert.isTrue(implementation != null,
                    "`implementation' attribute is required "
                            + "to create DiscoveryServiceProvider!");
            discoveyConfigBuilder.addPropertyReference("discoveryServiceProvider", implementation);
        }

        private void handleDiscoveryNodeFilter(Node node, BeanDefinitionBuilder discoveryConfigBuilder) {
            final NamedNodeMap attrs = node.getAttributes();
            Node classNameNode = attrs.getNamedItem("class-name");
            String className = classNameNode != null ? getTextContent(classNameNode) : null;
            Node implNode = attrs.getNamedItem("implementation");
            String implementation = implNode != null ? getTextContent(implNode) : null;
            Assert.isTrue(className != null || implementation != null,
                    "One of 'class-name' or 'implementation' attributes is required "
                            + "to create NodeFilter!");
            discoveryConfigBuilder.addPropertyValue("nodeFilterClass", className);
            if (implementation != null) {
                discoveryConfigBuilder.addPropertyReference("nodeFilter", implementation);
            }
        }

        private void handleDiscoveryStrategy(Node node, ManagedList discoveryStrategyConfigs) {
            final BeanDefinitionBuilder discoveryStrategyConfigBuilder =
                    createBeanBuilder(DiscoveryStrategyConfig.class);
            final NamedNodeMap attrs = node.getAttributes();
            Node classNameNode = attrs.getNamedItem("class-name");
            String className = classNameNode != null ? getTextContent(classNameNode) : null;
            Node implNode = attrs.getNamedItem("discovery-strategy-factory");
            String implementation = implNode != null ? getTextContent(implNode) : null;
            Assert.isTrue(className != null || implementation != null,
                    "One of 'class-name' or 'implementation' attributes is required "
                            + "to create DiscoveryStrategyConfig!");
            if (implementation != null) {
                discoveryStrategyConfigBuilder.addConstructorArgReference(implementation);
            } else if (className != null) {
                discoveryStrategyConfigBuilder.addConstructorArgValue(className);
            }

            for (Node child : childElements(node)) {
                final String name = cleanNodeName(child);
                if ("properties".equals(name)) {
                    ManagedMap properties = parseProperties(child);
                    if (!properties.isEmpty()) {
                        discoveryStrategyConfigBuilder.addConstructorArgValue(properties);
                    }
                }
            }
            discoveryStrategyConfigs.add(discoveryStrategyConfigBuilder.getBeanDefinition());
        }
    }
}
