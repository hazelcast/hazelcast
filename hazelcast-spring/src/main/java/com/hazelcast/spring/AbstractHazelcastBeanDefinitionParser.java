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

package com.hazelcast.spring;

import com.hazelcast.config.AbstractXmlConfigHelper;
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.spring.context.SpringManagedContext;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

/**
 * Base class of all Hazelcast BeanDefinitionParser implementations.
 * <p/>
 * <ul>
 *     <li>{@link com.hazelcast.spring.HazelcastClientBeanDefinitionParser}</li>
 *     <li>{@link com.hazelcast.spring.HazelcastConfigBeanDefinitionParser}</li>
 *     <li>{@link com.hazelcast.spring.HazelcastInstanceDefinitionParser}</li>
 *     <li>{@link com.hazelcast.spring.HazelcastTypeBeanDefinitionParser}</li>
 * </ul>
 *
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

        protected void createAndFillListedBean(Node node,
                                               final Class clazz,
                                               final String propertyName,
                                               final ManagedMap managedMap,
                                               String... excludeNames) {
            BeanDefinitionBuilder builder = createBeanBuilder(clazz);
            final AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();
            //"name"
            final Node attName = node.getAttributes().getNamedItem(propertyName);
            final String name = getTextContent(attName);
            builder.addPropertyValue("name", name);
            fillValues(node, builder, excludeNames);
            managedMap.put(name, beanDefinition);
        }

        protected void fillValues(Node node, BeanDefinitionBuilder builder, String... excludeNames) {
            Collection<String> epn = excludeNames != null && excludeNames.length > 0
                    ? new HashSet<String>(Arrays.asList(excludeNames)) : null;
            fillAttributeValues(node, builder, epn);
            for (Node n : new IterableNodeList(node, Node.ELEMENT_NODE)) {
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
            for (Node listenerNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
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
            for (Node instanceNode : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                final BeanDefinitionBuilder confBuilder = createBeanBuilder(proxyFactoryConfigClass);
                fillAttributeValues(instanceNode, confBuilder);
                list.add(confBuilder.getBeanDefinition());
            }
            return list;
        }


        protected void handleDataSerializableFactories(final Node node, final BeanDefinitionBuilder serializationConfigBuilder) {
            ManagedMap factories = new ManagedMap();
            ManagedMap<Integer, String> classNames = new ManagedMap<Integer, String>();
            for (Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
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

            ManagedList serializers = new ManagedList();
            for (Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
                final String name = cleanNodeName(child);
                if ("global-serializer".equals(name)) {
                    globalSerializerConfigBuilder =
                            createGSConfigBuilder(GlobalSerializerConfig.class, child, implementation, className);
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

        private BeanDefinitionBuilder createGSConfigBuilder(Class<GlobalSerializerConfig> globalSerializerConfigClass,
                                                            Node child, String implementation, String className) {
            BeanDefinitionBuilder globalSerializerConfigBuilder = createBeanBuilder(globalSerializerConfigClass);
            final NamedNodeMap attrs = child.getAttributes();
            final Node implRef = attrs.getNamedItem(implementation);
            final Node classNode = attrs.getNamedItem(className);
            if (implRef != null) {
                globalSerializerConfigBuilder.addPropertyReference(xmlToJavaName(implementation)
                        , getTextContent(implRef));
            }
            if (classNode != null) {
                globalSerializerConfigBuilder.addPropertyValue(xmlToJavaName(className), getTextContent(classNode));
            }

            return globalSerializerConfigBuilder;

        }

        protected void handlePortableFactories(final Node node, final BeanDefinitionBuilder serializationConfigBuilder) {
            ManagedMap factories = new ManagedMap();
            ManagedMap<Integer, String> classNames = new ManagedMap<Integer, String>();
            for (Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
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
            for (Node child : new IterableNodeList(node.getChildNodes())) {
                final String nodeName = cleanNodeName(child.getNodeName());
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
            for (Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
                final String name = cleanNodeName(child);
                if ("properties".equals(name)) {
                    handleProperties(child, socketInterceptorConfigBuilder);
                }
            }
            networkConfigBuilder.addPropertyValue("socketInterceptorConfig",
                    socketInterceptorConfigBuilder.getBeanDefinition());
        }

        protected void handleProperties(final Node node, BeanDefinitionBuilder beanDefinitionBuilder) {
            ManagedMap properties = new ManagedMap();
            for (Node n : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                final String name = cleanNodeName(n.getNodeName());
                final String propertyName;
                if (!"property".equals(name)) {
                    continue;
                }
                propertyName = getTextContent(n.getAttributes().getNamedItem("name")).trim();
                final String value = getTextContent(n);
                properties.put(propertyName, value);
            }
            beanDefinitionBuilder.addPropertyValue("properties", properties);
        }

        protected void handleSpringAware(final Node node) {
            boolean annotationDriven = true;
            for (org.w3c.dom.Node element : new IterableNodeList(node, Node.ELEMENT_NODE)) {
                final String nodeName = cleanNodeName(element.getNodeName());
                if ("spring-aware".equals(nodeName)) {
                    String enabled = getTextContent(element.getAttributes().getNamedItem("enabled")).trim();
                    if ("false".equals(enabled)) {
                        annotationDriven = false;
                    }
                    break;
                }
            }
            if (annotationDriven) {
                BeanDefinitionBuilder managedContextBeanBuilder = createBeanBuilder(SpringManagedContext.class);
                configBuilder.addPropertyValue("managedContext", managedContextBeanBuilder.getBeanDefinition());
            }
        }
    }
}
