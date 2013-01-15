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

import com.hazelcast.impl.FactoryImpl;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

public class HazelcastInstanceDefinitionParser extends AbstractHazelcastBeanDefinitionParser {

    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        final SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(parserContext);
        springXmlBuilder.handle(element);
        return springXmlBuilder.getBeanDefinition();
    }

    private class SpringXmlBuilder extends SpringXmlBuilderHelper {

        private final ParserContext parserContext;

        private BeanDefinitionBuilder builder;

        public SpringXmlBuilder(ParserContext parserContext) {
            this.parserContext = parserContext;
            this.builder = BeanDefinitionBuilder.rootBeanDefinition(FactoryImpl.class);
            this.builder.setFactoryMethod("newHazelcastInstanceProxy");
            this.builder.setDestroyMethodName("shutdown");
        }

        public AbstractBeanDefinition getBeanDefinition() {
            return builder.getBeanDefinition();
        }

        public void handle(Element element) {
            handleCommonBeanAttributes(element, builder, parserContext);
            for (org.w3c.dom.Node node : new IterableNodeList(element, Node.ELEMENT_NODE)) {
                final String nodeName = cleanNodeName(node.getNodeName());
                if ("config".equals(nodeName)) {
                    final HazelcastConfigBeanDefinitionParser configParser = new HazelcastConfigBeanDefinitionParser();
                    final AbstractBeanDefinition configBeanDef = configParser
                            .parseInternal((Element) node, parserContext);
                    this.builder.addConstructorArgValue(configBeanDef);
                }
            }
        }
    }
}
