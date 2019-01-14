/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.spring;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.spring.AbstractHazelcastBeanDefinitionParser;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import static org.springframework.beans.factory.support.BeanDefinitionBuilder.rootBeanDefinition;

/**
 * BeanDefinitionParser for Hazelcast Instance created via Jet Instance.
 * <p>
 * <b>Sample Spring XML for Hazelcast Instance created via Jet Instance:</b>
 * <pre>{@code
 * <jet:hazelcast jet-instance-ref="jet-instance" id="hazelcast-instance"/>
 * }</pre>
 */
public class JetHazelcastBeanDefinitionParser extends AbstractHazelcastBeanDefinitionParser {

    @Override
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        return new SpringXmlBuilder(parserContext).handle(element);
    }

    private class SpringXmlBuilder extends SpringXmlBuilderHelper {

        private final ParserContext parserContext;
        private final BeanDefinitionBuilder builder;

        SpringXmlBuilder(ParserContext parserContext) {
            this.parserContext = parserContext;
            this.builder = rootBeanDefinition(JetInstance.class).setFactoryMethod("getHazelcastInstance");
        }

        AbstractBeanDefinition handle(Element element) {
            handleCommonBeanAttributes(element, builder, parserContext);
            NamedNodeMap attributes = element.getAttributes();
            if (attributes != null) {
                Node jetInstanceRefNode = attributes.getNamedItem("jet-instance-ref");
                if (jetInstanceRefNode != null) {
                    String jetInstanceRef = getTextContent(jetInstanceRefNode);
                    builder.getRawBeanDefinition().setFactoryBeanName(jetInstanceRef);
                    return builder.addDependsOn(jetInstanceRef).getBeanDefinition();
                }
            }
            throw new IllegalStateException("'jet-instance-ref' attribute is required for creating HazelcastInstance");
        }

    }
}
