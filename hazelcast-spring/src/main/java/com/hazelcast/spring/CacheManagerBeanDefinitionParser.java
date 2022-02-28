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

import com.hazelcast.spring.cache.SpringHazelcastCachingProvider;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.util.Properties;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;

/**
 * Bean definition parser for JCache {@link javax.cache.CacheManager}.
 * <p>
 * <b>Sample bean</b>
 * <pre>
 * &lt;hz:cache-manager id="cacheManager" instance-ref="instance" name="cacheManager"/&gt;
 * </pre>
 */
public class CacheManagerBeanDefinitionParser extends AbstractHazelcastBeanDefinitionParser {

    @Override
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(SpringHazelcastCachingProvider.class, parserContext);
        springXmlBuilder.handle(element);
        BeanDefinitionBuilder builder = springXmlBuilder.getBuilder();
        return builder.getBeanDefinition();
    }

    private class SpringXmlBuilder extends AbstractHazelcastBeanDefinitionParser.SpringXmlBuilderHelper {

        private final ParserContext parserContext;
        private final BeanDefinitionBuilder builder;

        SpringXmlBuilder(Class providerClass, ParserContext parserContext) {
            this.parserContext = parserContext;
            this.builder = BeanDefinitionBuilder.rootBeanDefinition(providerClass);
        }

        BeanDefinitionBuilder getBuilder() {
            return this.builder;
        }

        public void handle(Element element) {
            handleCommonBeanAttributes(element, builder, parserContext);
            NamedNodeMap attributes = element.getAttributes();

            String uri = null;
            String instanceRef = null;
            if (attributes != null) {
                Node instanceRefNode = attributes.getNamedItem("instance-ref");
                if (instanceRefNode != null) {
                    instanceRef = getTextContent(instanceRefNode);
                }
                Node uriNode = attributes.getNamedItem("uri");
                if (uriNode != null) {
                    uri = getTextContent(uriNode);
                }
            }

            Properties properties = new Properties();
            for (Node n : childElements(element)) {
                String nodeName = cleanNodeName(n);
                if ("properties".equals(nodeName)) {
                    for (Node propNode : childElements(n)) {
                        String name = cleanNodeName(propNode);
                        String propertyName;
                        if (!"property".equals(name)) {
                            continue;
                        }
                        propertyName = getTextContent(propNode.getAttributes().getNamedItem("name")).trim();
                        String value = getTextContent(propNode);
                        properties.setProperty(propertyName, value);
                    }
                }
            }

            if (instanceRef != null) {
                builder.addConstructorArgReference(instanceRef);
            }
            builder.addConstructorArgValue(uri);
            builder.addConstructorArgValue(properties);
            builder.setFactoryMethod("getCacheManager");
        }
    }
}
