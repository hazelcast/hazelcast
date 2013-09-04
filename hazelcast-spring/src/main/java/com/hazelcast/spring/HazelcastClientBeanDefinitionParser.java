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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ProxyFactoryConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.util.RandomLB;
import com.hazelcast.client.util.RoundRobinLB;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.spring.context.SpringManagedContext;
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

public class HazelcastClientBeanDefinitionParser extends AbstractHazelcastBeanDefinitionParser {

    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        final SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(parserContext);
        springXmlBuilder.handleClient(element);
        return springXmlBuilder.getBeanDefinition();
    }

    private class SpringXmlBuilder extends SpringXmlBuilderHelper {

        private final ParserContext parserContext;

        private BeanDefinitionBuilder builder;


        private ManagedMap nearCacheConfigMap;//= new HashMap<String, NearCacheConfig>();

        public SpringXmlBuilder(ParserContext parserContext) {
            this.parserContext = parserContext;
            this.builder = BeanDefinitionBuilder.rootBeanDefinition(HazelcastClient.class);
            this.builder.setFactoryMethod("newHazelcastClient");
            this.builder.setDestroyMethodName("shutdown");
            this.nearCacheConfigMap =  new ManagedMap();

            this.configBuilder = BeanDefinitionBuilder.rootBeanDefinition(ClientConfig.class);
            configBuilder.addPropertyValue("nearCacheConfigMap", nearCacheConfigMap);

            BeanDefinitionBuilder managedContextBeanBuilder = createBeanBuilder(SpringManagedContext.class);
            this.configBuilder.addPropertyValue("managedContext", managedContextBeanBuilder.getBeanDefinition());
        }

        public AbstractBeanDefinition getBeanDefinition() {
            return builder.getBeanDefinition();
        }

        public void handleClient(Element element) {
            handleCommonBeanAttributes(element, builder, parserContext);
            final NamedNodeMap attrs = element.getAttributes();
            if (attrs != null) {
                for (int a = 0; a < attrs.getLength(); a++) {
                    final org.w3c.dom.Node att = attrs.item(a);
                    final String name = att.getNodeName();
                    final String value = att.getNodeValue();
                    if ("executor-pool-size".equals(name)) {
                        configBuilder.addPropertyValue("executorPoolSize", value);
                    } else if ("credentials-ref".equals(name)) {
                        configBuilder.addPropertyReference("credentials", value);
                    }
                }
            }
            for (org.w3c.dom.Node node : new IterableNodeList(element, Node.ELEMENT_NODE)) {
                final String nodeName = cleanNodeName(node.getNodeName());
                if ("group".equals(nodeName)) {
                    createAndFillBeanBuilder(node, GroupConfig.class, "groupConfig", configBuilder);
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
                } else if ("socket-interceptor".equals(nodeName)) {
                    handleSocketInterceptorConfig(node, configBuilder);
                } else if ("load-balancer".equals(nodeName)) {
                    handleLoadBalacer(node);
                } else if ("near-cache".equals(nodeName)) {
                    handleNearCache(node);
                }
            }
            builder.addConstructorArgValue(configBuilder.getBeanDefinition());
        }

         private void handleNetwork(Node node) {
            List<String> members = new ArrayList<String>(10);
            fillAttributeValues(node, configBuilder);
            for (org.w3c.dom.Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
                final String nodeName = cleanNodeName(child);
                if ("member".equals(nodeName)) {
                    members.add(getTextContent(child));
                } else if("socket-options".equals(nodeName)) {
                    createAndFillBeanBuilder(child, SocketOptions.class,"socketOptions", configBuilder);
                }
            }
            configBuilder.addPropertyValue("addresses", members);
        }

        private void handleLoadBalacer(Node node) {
            final String type = getAttribute(node, "type");
            if ("random".equals(type)) {
                configBuilder.addPropertyValue("loadBalancer", new RandomLB());
            } else if ("round-robin".equals(type)) {
                configBuilder.addPropertyValue("loadBalancer", new RoundRobinLB());
            }
        }

        private void handleNearCache(Node node) {
            createAndFillListedBean(node, NearCacheConfig.class, "name", nearCacheConfigMap,"name");
        }

    }


}

