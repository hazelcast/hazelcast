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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.ProxyFactoryConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.util.RandomLB;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.SSLConfig;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.util.StringUtil.upperCaseInternal;

/**
 * BeanDefinitionParser for Hazelcast Client Configuration
 * <p/>
 * <p/>
 * <b>Sample Spring XML for Hazelcast Client:</b>
 * <pre>
 * &lt;hz:client id="client"&gt;
 *  &lt;hz:group name="${cluster.group.name}" password="${cluster.group.password}" /&gt;
 *  &lt;hz:network connection-attempt-limit="3"
 *      connection-attempt-period="3000"
 *      connection-timeout="1000"
 *      redo-operation="true"
 *      smart-routing="true"&gt;
 *          &lt;hz:member&gt;10.10.1.2:5701&lt;/hz:member&gt;
 *          &lt;hz:member&gt;10.10.1.3:5701&lt;/hz:member&gt;
 *  &lt;/hz:network&gt;
 * &lt;/hz:client&gt;
 * </pre>
 */
public class HazelcastClientBeanDefinitionParser extends AbstractHazelcastBeanDefinitionParser {

    private static final int INITIAL_CAPACITY = 10;

    @Override
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        final SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(parserContext);
        springXmlBuilder.handleClient(element);
        return springXmlBuilder.getBeanDefinition();
    }

    private class SpringXmlBuilder extends SpringXmlBuilderHelper {

        private final ParserContext parserContext;

        private BeanDefinitionBuilder builder;

        //= new HashMap<String, NearCacheConfig>();
        private ManagedMap nearCacheConfigMap;

        public SpringXmlBuilder(ParserContext parserContext) {
            this.parserContext = parserContext;
            this.builder = BeanDefinitionBuilder.rootBeanDefinition(HazelcastClient.class);
            this.builder.setFactoryMethod("newHazelcastClient");
            this.builder.setDestroyMethodName("shutdown");
            this.nearCacheConfigMap = new ManagedMap();

            this.configBuilder = BeanDefinitionBuilder.rootBeanDefinition(ClientConfig.class);
            configBuilder.addPropertyValue("nearCacheConfigMap", nearCacheConfigMap);
        }

        public AbstractBeanDefinition getBeanDefinition() {
            return builder.getBeanDefinition();
        }

        public void handleClient(Element element) {
            handleCommonBeanAttributes(element, builder, parserContext);
            handleClientAttributes(element);
            for (Node node : childElements(element)) {
                final String nodeName = cleanNodeName(node);
                if ("group".equals(nodeName)) {
                    createAndFillBeanBuilder(node, GroupConfig.class, "groupConfig", configBuilder);
                } else if ("properties".equals(nodeName)) {
                    handleProperties(node, configBuilder);
                } else if ("network".equals(nodeName)) {
                    handleNetwork(node);
                } else if ("listeners".equals(nodeName)) {
                    final List listeners = parseListeners(node, ListenerConfig.class);
                    configBuilder.addPropertyValue("listenerConfigs", listeners);
                } else if ("serialization".equals(nodeName)) {
                    handleSerialization(node);
                } else if ("proxy-factories".equals(nodeName)) {
                    final List list = parseProxyFactories(node, ProxyFactoryConfig.class);
                    configBuilder.addPropertyValue("proxyFactoryConfigs", list);
                } else if ("load-balancer".equals(nodeName)) {
                    handleLoadBalancer(node);
                } else if ("near-cache".equals(nodeName)) {
                    handleNearCache(node);
                } else if ("spring-aware".equals(nodeName)) {
                    handleSpringAware();
                } else if ("query-caches".equals(nodeName)) {
                    ManagedMap queryCaches = getQueryCaches(node);
                    configBuilder.addPropertyValue("queryCacheConfigs", queryCaches);
                }
            }
            builder.addConstructorArgValue(configBuilder.getBeanDefinition());
        }

        private void handleClientAttributes(Element element) {
            final NamedNodeMap attrs = element.getAttributes();
            if (attrs != null) {
                for (int a = 0; a < attrs.getLength(); a++) {
                    final Node att = attrs.item(a);
                    final String name = att.getNodeName();
                    final String value = att.getNodeValue();
                    if ("executor-pool-size".equals(name)) {
                        configBuilder.addPropertyValue("executorPoolSize", value);
                    } else if ("credentials-ref".equals(name)) {
                        configBuilder.addPropertyReference("credentials", value);
                    }
                }
            }
        }

        private void handleNetwork(Node node) {
            final BeanDefinitionBuilder clientNetworkConfig = createBeanBuilder(ClientNetworkConfig.class);
            List<String> members = new ArrayList<String>(INITIAL_CAPACITY);
            fillAttributeValues(node, clientNetworkConfig);
            for (Node child : childElements(node)) {
                final String nodeName = cleanNodeName(child);
                if ("member".equals(nodeName)) {
                    members.add(getTextContent(child));
                } else if ("socket-options".equals(nodeName)) {
                    createAndFillBeanBuilder(child, SocketOptions.class, "socketOptions", clientNetworkConfig);
                } else if ("socket-interceptor".equals(nodeName)) {
                    handleSocketInterceptorConfig(child, clientNetworkConfig);
                } else if ("ssl".equals(nodeName)) {
                    handleSSLConfig(child, clientNetworkConfig);
                } else if ("aws".equals(nodeName)) {
                    handleAws(child, clientNetworkConfig);
                } else if ("discovery-strategies".equals(nodeName)) {
                    handleDiscoveryStrategies(child, clientNetworkConfig);
                }
            }
            clientNetworkConfig.addPropertyValue("addresses", members);

            configBuilder.addPropertyValue("networkConfig", clientNetworkConfig.getBeanDefinition());
        }

        private void handleAws(Node node, BeanDefinitionBuilder clientNetworkConfig) {
            createAndFillBeanBuilder(node, ClientAwsConfig.class, "awsConfig", clientNetworkConfig);
        }

        private void handleSSLConfig(final Node node, final BeanDefinitionBuilder networkConfigBuilder) {
            BeanDefinitionBuilder sslConfigBuilder = createBeanBuilder(SSLConfig.class);
            final String implAttribute = "factory-implementation";
            fillAttributeValues(node, sslConfigBuilder, implAttribute);
            Node implNode = node.getAttributes().getNamedItem(implAttribute);
            String implementation = implNode != null ? getTextContent(implNode) : null;
            if (implementation != null) {
                sslConfigBuilder.addPropertyReference(xmlToJavaName(implAttribute), implementation);
            }
            for (Node child : childElements(node)) {
                final String name = cleanNodeName(child);
                if ("properties".equals(name)) {
                    handleProperties(child, sslConfigBuilder);
                }
            }
            networkConfigBuilder.addPropertyValue("SSLConfig", sslConfigBuilder.getBeanDefinition());
        }

        private void handleLoadBalancer(Node node) {
            final String type = getAttribute(node, "type");
            if ("random".equals(type)) {
                configBuilder.addPropertyValue("loadBalancer", new RandomLB());
            } else if ("round-robin".equals(type)) {
                configBuilder.addPropertyValue("loadBalancer", new RoundRobinLB());
            }
        }

        private void handleNearCache(Node node) {
            createAndFillListedBean(node, NearCacheConfig.class, "name", nearCacheConfigMap, "name");
        }

        private ManagedMap getQueryCaches(Node childNode) {
            ManagedMap queryCaches = new ManagedMap();
            for (Node queryCacheNode : childElements(childNode)) {
                parseQueryCache(queryCaches, queryCacheNode);
            }
            return queryCaches;
        }

        private BeanDefinitionBuilder parseQueryCache(ManagedMap queryCaches,
                                                      Node queryCacheNode) {
            final BeanDefinitionBuilder builder = createBeanBuilder(QueryCacheConfig.class);

            NamedNodeMap attributes = queryCacheNode.getAttributes();
            String mapName = getTextContent(attributes.getNamedItem("map-name"));
            String cacheName = getTextContent(attributes.getNamedItem("name"));

            for (Node node : childElements(queryCacheNode)) {
                String nodeName = cleanNodeName(node);
                String textContent = getTextContent(node);

                parseQueryCacheInternal(builder, node, nodeName, textContent);
            }

            builder.addPropertyValue("name", cacheName);

            ManagedMap configMap = (ManagedMap) queryCaches.get(mapName);
            if (configMap == null) {
                configMap = new ManagedMap<String, QueryCacheConfig>();
                queryCaches.put(mapName, configMap);
            }
            configMap.put(cacheName, builder.getBeanDefinition());


            return builder;
        }

        private void parseQueryCacheInternal(BeanDefinitionBuilder builder, Node node, String nodeName, String textContent) {
            if ("predicate".equals(nodeName)) {
                BeanDefinitionBuilder predicateBuilder = getPredicate(node, textContent);
                builder.addPropertyValue("predicateConfig", predicateBuilder.getBeanDefinition());
            } else if ("entry-listeners".equals(nodeName)) {
                ManagedList listeners = getEntryListeners(node);
                builder.addPropertyValue("entryListenerConfigs", listeners);
            } else if ("include-value".equals(nodeName)) {
                boolean includeValue = getBooleanValue(textContent);
                builder.addPropertyValue("includeValue", includeValue);
            } else if ("batch-size".equals(nodeName)) {
                int batchSize = getIntegerValue("batch-size", textContent.trim()
                );
                builder.addPropertyValue("batchSize", batchSize);
            } else if ("buffer-size".equals(nodeName)) {
                int bufferSize = getIntegerValue("buffer-size", textContent.trim()
                );
                builder.addPropertyValue("bufferSize", bufferSize);
            } else if ("delay-seconds".equals(nodeName)) {
                int delaySeconds = getIntegerValue("delay-seconds", textContent.trim()
                );
                builder.addPropertyValue("delaySeconds", delaySeconds);
            } else if ("in-memory-format".equals(nodeName)) {
                String value = textContent.trim();
                builder.addPropertyValue("inMemoryFormat", InMemoryFormat.valueOf(upperCaseInternal(value)));
            } else if ("coalesce".equals(nodeName)) {
                boolean coalesce = getBooleanValue(textContent);
                builder.addPropertyValue("coalesce", coalesce);
            } else if ("populate".equals(nodeName)) {
                boolean populate = getBooleanValue(textContent);
                builder.addPropertyValue("populate", populate);
            } else if ("indexes".equals(nodeName)) {
                ManagedList indexes = getIndexes(node);
                builder.addPropertyValue("indexConfigs", indexes);
            } else if ("eviction".equals(nodeName)) {
                builder.addPropertyValue("evictionConfig", getEvictionConfig(node));
            }
        }

        private BeanDefinitionBuilder getPredicate(Node node, String textContent) {
            BeanDefinitionBuilder predicateBuilder = createBeanBuilder(PredicateConfig.class);
            String predicateType = getTextContent(node.getAttributes().getNamedItem("type"));
            if ("sql".equals(predicateType)) {
                predicateBuilder.addPropertyValue("sql", textContent);
            } else if ("class-name".equals(predicateType)) {
                predicateBuilder.addPropertyValue("className", textContent);
            }
            return predicateBuilder;
        }

        private ManagedList getIndexes(Node node) {
            ManagedList indexes = new ManagedList();
            for (Node indexNode : childElements(node)) {
                final BeanDefinitionBuilder indexConfBuilder = createBeanBuilder(MapIndexConfig.class);
                fillAttributeValues(indexNode, indexConfBuilder);
                indexes.add(indexConfBuilder.getBeanDefinition());
            }
            return indexes;
        }

        private ManagedList getEntryListeners(Node node) {
            ManagedList listeners = new ManagedList();
            final String implementationAttr = "implementation";
            for (Node listenerNode : childElements(node)) {
                BeanDefinitionBuilder listenerConfBuilder = createBeanBuilder(EntryListenerConfig.class);
                fillAttributeValues(listenerNode, listenerConfBuilder, implementationAttr);
                Node implementationNode = listenerNode.getAttributes().getNamedItem(implementationAttr);
                if (implementationNode != null) {
                    listenerConfBuilder.addPropertyReference(implementationAttr, getTextContent(implementationNode));
                }
                listeners.add(listenerConfBuilder.getBeanDefinition());
            }
            return listeners;
        }
    }
}

