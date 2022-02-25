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

import com.hazelcast.core.HazelcastInstance;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import static com.hazelcast.spring.HazelcastInstanceDefinitionParser.CP_SUBSYSTEM_SUFFIX;

/**
 * BeanDefinitionParser for Type Configuration of Hazelcast Types like map, queue, topic etc.
 */
public class HazelcastTypeBeanDefinitionParser extends AbstractHazelcastBeanDefinitionParser {

    private final String type;
    private final String methodName;

    public HazelcastTypeBeanDefinitionParser(String type) {
        this.type = type;
        this.methodName = "get" + Character.toUpperCase(type.charAt(0)) + type.substring(1);
    }

    @Override
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(parserContext);
        springXmlBuilder.handle(element);
        BeanDefinitionBuilder builder = springXmlBuilder.getBuilder();
        builder.setFactoryMethod(methodName);
        return builder.getBeanDefinition();
    }

    private class SpringXmlBuilder extends SpringXmlBuilderHelper {

        private final ParserContext parserContext;
        private final BeanDefinitionBuilder builder;

        SpringXmlBuilder(ParserContext parserContext) {
            this.parserContext = parserContext;
            this.builder = BeanDefinitionBuilder.rootBeanDefinition(HazelcastInstance.class);
        }

        BeanDefinitionBuilder getBuilder() {
            return this.builder;
        }

        public void handle(Element element) {
            handleCommonBeanAttributes(element, builder, parserContext);
            NamedNodeMap attributes = element.getAttributes();
            if (attributes != null) {
                Node instanceRefNode = attributes.getNamedItem("instance-ref");
                if (instanceRefNode == null) {
                    throw new IllegalStateException("'instance-ref' attribute is required for creating Hazelcast " + type);
                }
                String instanceRef = getTextContent(instanceRefNode);
                if (HazelcastNamespaceHandler.CP_TYPES.contains(type)) {
                    builder.getRawBeanDefinition().setFactoryBeanName(instanceRef + CP_SUBSYSTEM_SUFFIX);
                } else {
                    builder.getRawBeanDefinition().setFactoryBeanName(instanceRef);
                }
                builder.addDependsOn(instanceRef);

                Node nameNode = attributes.getNamedItem("name");
                if (nameNode == null) {
                    nameNode = attributes.getNamedItem("id");
                }
                builder.addConstructorArgValue(getTextContent(nameNode));
            }
        }
    }
}
