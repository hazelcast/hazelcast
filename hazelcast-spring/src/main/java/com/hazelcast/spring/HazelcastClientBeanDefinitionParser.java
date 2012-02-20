/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.AbstractXmlConfigHelper;
import com.hazelcast.config.GroupConfig;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

public class HazelcastClientBeanDefinitionParser extends AbstractBeanDefinitionParser {

    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        final SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(parserContext);
        springXmlBuilder.handle(element);
        return springXmlBuilder.getBeanDefinition();
    }

    private static class SpringXmlBuilder extends AbstractXmlConfigHelper {

        private final ParserContext parserContext;

        private BeanDefinitionBuilder builder;

        private ManagedList members;

        private BeanDefinitionBuilder configBuilder;

        private BeanDefinitionBuilder groupConfigBuilder;

        public SpringXmlBuilder(ParserContext parserContext) {
            this.parserContext = parserContext;
            this.builder = BeanDefinitionBuilder.rootBeanDefinition(HazelcastClient.class);
            this.builder.setFactoryMethod("newHazelcastClient");
            this.builder.setDestroyMethodName("shutdown");
            this.members = new ManagedList();
            this.configBuilder = createBeanBuilder(ClientConfig.class, "client-config");
            this.groupConfigBuilder = createBeanBuilder(GroupConfig.class, "client-group-config");
            configBuilder.addPropertyValue("groupConfig", groupConfigBuilder.getBeanDefinition());
        }

        protected BeanDefinitionBuilder createBeanBuilder(final Class clazz, final String id) {
            BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(clazz);
            final AbstractBeanDefinition beanDefinition = builder.getBeanDefinition();
            BeanDefinitionHolder holder = new BeanDefinitionHolder(beanDefinition, id);
            BeanDefinitionReaderUtils.registerBeanDefinition(holder, parserContext.getRegistry());
            return builder;
        }

        public AbstractBeanDefinition getBeanDefinition() {
            return builder.getBeanDefinition();
        }

        public void handle(Element element) {
            final NamedNodeMap attrs = element.getAttributes();
            if (attrs != null) {
                for (int a = 0; a < attrs.getLength(); a++) {
                    final org.w3c.dom.Node att = attrs.item(a);
                    final String name = att.getNodeName();
                    final String value = att.getNodeValue();
                    if ("group-name".equals(name)) {
                        groupConfigBuilder.addPropertyValue("name", value);
                    } else if ("group-password".equals(name)) {
                        groupConfigBuilder.addPropertyValue("password", value);
                    } else if ("auto-update-members".equals(name)) {
                        configBuilder.addPropertyValue("updateAutomatic", value);
                    } else if ("shuffle-members".equals(name)) {
                        configBuilder.addPropertyValue("shuffle", value);
                    } else if ("connect-attempt-limit".equals(name)) {
                        configBuilder.addPropertyValue("initialConnectionAttemptLimit", value);
                    } else if ("connect-timeout".equals(name)) {
                        configBuilder.addPropertyValue("connectionTimeout", value);
                    } else if ("reconnect-attempt-limit".equals(name)) {
                        configBuilder.addPropertyValue("reconnectionAttemptLimit", value);
                    } else if ("reconnect-timeout".equals(name)) {
                        configBuilder.addPropertyValue("reConnectionTimeOut", value);
                    } else if ("credentials-ref".equals(name)) {
                        configBuilder.addPropertyReference("credentials", value);
                    }
                }
            }
            for (org.w3c.dom.Node node : new IterableNodeList(element, Node.ELEMENT_NODE)) {
                final String nodeName = cleanNodeName(node.getNodeName());
                if ("member".equals(nodeName)) {
                    members.add(getValue(node));
                }
            }
            configBuilder.addPropertyValue("addresses", members);
            builder.addConstructorArgValue(configBuilder.getBeanDefinition());
        }
    }
}
