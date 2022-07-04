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

package com.hazelcast.spring.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spring.AbstractHazelcastBeanDefinitionParser;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import javax.annotation.Nonnull;

// TODO: I directly replaced JetInstance with JetService for
//  my convenience. If necessary, add the JetInstance bean.
/**
 * Bean definition parser for {@link com.hazelcast.jet.JetService}.
 * <p>
 * <b>Sample bean</b>
 * <pre>
 * &lt;hz:jet id="jetService" instance-ref="instance"/&gt;
 * </pre>
 */
public class JetBeanDefinitionParser extends AbstractHazelcastBeanDefinitionParser {

    private static final String METHOD_NAME = "getJet";

    @Override
    protected AbstractBeanDefinition parseInternal(@Nonnull Element element, @Nonnull ParserContext parserContext) {
        SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(parserContext);
        springXmlBuilder.handle(element);
        BeanDefinitionBuilder builder = springXmlBuilder.getBuilder();
        builder.setFactoryMethod(METHOD_NAME);
        return builder.getBeanDefinition();
    }

    private static class SpringXmlBuilder extends SpringXmlBuilderHelper {

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
                    throw new IllegalStateException("'instance-ref' attribute is required for creating JetInstance");
                }
                String instanceRef = getTextContent(instanceRefNode);
                builder.getRawBeanDefinition().setFactoryBeanName(instanceRef);
                builder.addDependsOn(instanceRef);
            }
        }
    }
}
