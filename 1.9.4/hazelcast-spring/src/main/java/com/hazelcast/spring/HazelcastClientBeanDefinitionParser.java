/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.spring;

import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.hazelcast.client.ClientProperties;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.AbstractXmlConfigHelper;


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
        
        private BeanDefinitionBuilder propertiesBuilder;

        public SpringXmlBuilder(ParserContext parserContext) {
            this.parserContext = parserContext;
            this.builder = BeanDefinitionBuilder.rootBeanDefinition(HazelcastClient.class);
            this.builder.setFactoryMethod("newHazelcastClient");
            this.builder.setDestroyMethodName("shutdown");
            
            this.members = new ManagedList();
            this.propertiesBuilder = BeanDefinitionBuilder.rootBeanDefinition(ClientProperties.class);
            
            final AbstractBeanDefinition beanDefinition = propertiesBuilder.getBeanDefinition();
            BeanDefinitionHolder holder = new BeanDefinitionHolder(beanDefinition, "client-properties");
            
            BeanDefinitionReaderUtils.registerBeanDefinition(holder, parserContext.getRegistry());
        }

        public AbstractBeanDefinition getBeanDefinition() {
            return builder.getBeanDefinition();
        }

        public void handle(Element element) {
            ManagedMap properties = new ManagedMap();
            final NamedNodeMap atts = element.getAttributes();
            if (atts != null) {
                for (int a = 0; a < atts.getLength(); a++) {
                    final org.w3c.dom.Node att = atts.item(a);
                    String name = att.getNodeName();
                    final String value = att.getNodeValue();
                    if ("group-name".equals(name)){
                        name = ClientProperties.ClientPropertyName.GROUP_NAME.getName();
                    } else if ("group-password".equals(name)){
                        name = ClientProperties.ClientPropertyName.GROUP_PASSWORD.getName();
                    } else {
                        continue;
                    }
                    
                    properties.put(name, value);
                }
            }
            
            for (org.w3c.dom.Node node : new IterableNodeList(element, Node.ELEMENT_NODE)) {
                final String nodeName = cleanNodeName(node.getNodeName());
                if ("members".equals(nodeName)) {
                    members.add(getValue(node));
                } else if ("client-properties".equals(nodeName)){
                    for (org.w3c.dom.Node n : new IterableNodeList(node.getChildNodes(), Node.ELEMENT_NODE)) {
                        final String name = cleanNodeName(n.getNodeName());
                        final String propertyName;
                        if ("client-property".equals(name)){
                            propertyName = getTextContent(n.getAttributes().getNamedItem("name")).trim();
                            final String value = getValue(n);
                            properties.put(propertyName, value);
                        }
                    }
                }
            }
            
            propertiesBuilder.addPropertyValue("properties", properties);
            
            this.builder.addConstructorArgValue(propertiesBuilder.getBeanDefinition());
            this.builder.addConstructorArgValue(members);
        }
    }

}
