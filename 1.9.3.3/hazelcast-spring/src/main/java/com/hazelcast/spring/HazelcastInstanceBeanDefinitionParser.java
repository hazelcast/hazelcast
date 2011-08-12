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

import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;

import com.hazelcast.config.AbstractXmlConfigHelper;
import com.hazelcast.core.HazelcastInstance;


public class HazelcastInstanceBeanDefinitionParser extends AbstractBeanDefinitionParser {
    
    private final String methodName;
    
    public HazelcastInstanceBeanDefinitionParser(final String type) {
        this.methodName = "get" + Character.toUpperCase(type.charAt(0)) + type.substring(1);
    }
    
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        final SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(parserContext);
        springXmlBuilder.handle(element);
        final BeanDefinitionBuilder builder = springXmlBuilder.getBuilder();
        builder.setFactoryMethod(methodName);
        return builder.getBeanDefinition();
    }
    
    private static class SpringXmlBuilder extends AbstractXmlConfigHelper {
    
        private final ParserContext parserContext;
        
        private BeanDefinitionBuilder builder;

        public SpringXmlBuilder(ParserContext parserContext) {
            this.parserContext = parserContext;
            this.builder = BeanDefinitionBuilder.rootBeanDefinition(HazelcastInstance.class);
        }
    
        public BeanDefinitionBuilder getBuilder() {
            return this.builder;
        }
    
        public void handle(Element element) {
            final NamedNodeMap atts = element.getAttributes();
            if (atts != null) {
                for (int a = 0; a < atts.getLength(); a++) {
                    final org.w3c.dom.Node att = atts.item(a);
                    String name = att.getNodeName();
                    final String value = att.getNodeValue();
                    if ("name".equals(name)){
                        this.builder.addConstructorArgValue(value);
                    } else if ("instance-ref".equals(name)){
                        this.builder.getRawBeanDefinition().setFactoryBeanName(value);
                    } else {
                        continue;
                    }
                }
            }
        }
    }
}
