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
import com.hazelcast.client.config.SocketOptions;
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


        private ManagedMap cacheConfigMap ;//= new HashMap<String, NearCacheConfig>();

        public SpringXmlBuilder(ParserContext parserContext) {
            this.parserContext = parserContext;
            this.builder = BeanDefinitionBuilder.rootBeanDefinition(HazelcastClient.class);
            this.builder.setFactoryMethod("newHazelcastClient");
            this.builder.setDestroyMethodName("shutdown");
            this.cacheConfigMap =  new ManagedMap();

            this.configBuilder = BeanDefinitionBuilder.rootBeanDefinition(ClientConfig.class);
            configBuilder.addPropertyValue("cacheConfigMap", cacheConfigMap);

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
                    } else if ("smart".equals(name)) {
                        configBuilder.addPropertyValue("smart", value);
                    } else if ("redo-operation".equals(name)) {
                        configBuilder.addPropertyValue("redoOperation", value);
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
                    //TODO Listeners
//                    final List listeners = parseListeners(node, ListenerConfig.class);
//                    configBuilder.addPropertyValue("listenerConfigs", listeners);
                } else if ("serialization".equals(nodeName)) {
                    handleSerialization(node);
                } else if ("proxy-factories".equals(nodeName)) {
                    handleProxyFactories(node);
                } else if ("socket-interceptor".equals(nodeName)) {
                    handleSocketInterceptor(node);
                } else if ("load-balancer".equals(nodeName)) {
                    handleLoadBalacer(node);
                } else if ("near-cache".equals(nodeName)) {
                    handleNearCache(node);
                }
            }
            builder.addConstructorArgValue(configBuilder.getBeanDefinition());
        }

         private void handleNetwork(Node node) {
            ManagedList members = new ManagedList();
            for (org.w3c.dom.Node child : new IterableNodeList(node, Node.ELEMENT_NODE)) {
                final String nodeName = cleanNodeName(child);
                if ("member".equals(nodeName)) {
                    members.add(getTextContent(node));
                } else if("socket-options".equals(nodeName)) {
                    createAndFillBeanBuilder(node, SocketOptions.class,"socketOptions",configBuilder);
                }
            }
            configBuilder.addPropertyValue("addresses", members);
        }

        private void handleProxyFactories(Node node) {
            //To change body of created methods use File | Settings | File Templates.
        }

        private void handleSocketInterceptor(Node node) {
            //To change body of created methods use File | Settings | File Templates.
        }

        private void handleLoadBalacer(Node node) {
            //To change body of created methods use File | Settings | File Templates.
        }

        private void handleNearCache(Node node) {
            createAndFillListedBean(node, NearCacheConfig.class, "name", cacheConfigMap);
        }


    }


}

