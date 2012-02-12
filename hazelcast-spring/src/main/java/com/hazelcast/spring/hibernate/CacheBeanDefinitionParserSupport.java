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

package com.hazelcast.spring.hibernate;

import com.hazelcast.config.AbstractXmlConfigHelper;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

public abstract class CacheBeanDefinitionParserSupport extends AbstractBeanDefinitionParser {

    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        final SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(parserContext);
        springXmlBuilder.handle(element);
        return springXmlBuilder.getBeanDefinition();
    }

    private class SpringXmlBuilder extends AbstractXmlConfigHelper {

        private BeanDefinitionBuilder builder;

        public SpringXmlBuilder(ParserContext parserContext) {
            this.builder = createBeanDefinitionBuilder();
        }

        public AbstractBeanDefinition getBeanDefinition() {
            return builder.getBeanDefinition();
        }

        public void handle(Element element) {
            final NamedNodeMap atts = element.getAttributes();
            String instanceRefName = "instance";
            if (atts != null) {
                for (int a = 0; a < atts.getLength(); a++) {
                    final Node att = atts.item(a);
                    final String name = att.getNodeName();
                    if ("instance-ref".equals(name)) {
                        instanceRefName = att.getNodeValue();
                        break;
                    }
                }
            }
            this.builder.addConstructorArgReference(instanceRefName);
        }
    }

    protected abstract BeanDefinitionBuilder createBeanDefinitionBuilder();
}
