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

import com.hazelcast.core.HazelcastInstance;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

public class HazelcastTypeBeanDefinitionParser extends AbstractHazelcastBeanDefinitionParser {

    private final String type;
    private final String methodName;

    public HazelcastTypeBeanDefinitionParser(final String type) {
        super();
        this.type = type;
        this.methodName = "get" + Character.toUpperCase(type.charAt(0)) + type.substring(1);
    }

    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        final SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(parserContext);
        springXmlBuilder.handle(element);
        final BeanDefinitionBuilder builder = springXmlBuilder.getBuilder();
        builder.setFactoryMethod(methodName);
        return builder.getBeanDefinition();
    }

    private class SpringXmlBuilder extends SpringXmlBuilderHelper {

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
            handleCommonBeanAttributes(element, builder, parserContext);
            final NamedNodeMap attrs = element.getAttributes();
            if (attrs != null) {
                Node instanceRefNode = attrs.getNamedItem("instance-ref");
                if (instanceRefNode == null) {
                    throw new IllegalStateException("'instance-ref' attribute is required for creating"
                            + " Hazelcast "
                            + type);
                }
                final String instanceRef = getTextContent(instanceRefNode);
                builder.getRawBeanDefinition().setFactoryBeanName(instanceRef);
                builder.addDependsOn(instanceRef);

                Node nameNode = attrs.getNamedItem("name");
                if (nameNode == null) {
                    nameNode = attrs.getNamedItem("id");
                }
                builder.addConstructorArgValue(getTextContent(nameNode));
            }
        }
    }
}
