/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Bean definition parser for JCache {@link javax.cache.CacheManager}
 * <p/>
 * <b>Sample bean</b>
 * <pre>
 * &lt;hz:cache-manager id="cacheManager" instance-ref="instance" name="cacheManager" /&gt;
 * </pre>
 */
public class CacheManagerBeanDefinitionParser
        extends AbstractHazelcastBeanDefinitionParser {

    @Override
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        final SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(SpringHazelcastCachingProvider.class, parserContext);
        springXmlBuilder.handle(element);
        final BeanDefinitionBuilder builder = springXmlBuilder.getBuilder();
        return builder.getBeanDefinition();
    }

    private class SpringXmlBuilder extends AbstractHazelcastBeanDefinitionParser.SpringXmlBuilderHelper {

        private final ParserContext parserContext;

        private BeanDefinitionBuilder builder;

        public SpringXmlBuilder(Class providerClass, ParserContext parserContext) {
            this.parserContext = parserContext;
            this.builder = BeanDefinitionBuilder.rootBeanDefinition(providerClass);
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
                    throw new IllegalStateException("'instance-ref' attribute is required for creating cache manager");
                }
                final String instanceRef = getTextContent(instanceRefNode);

                Node uriNode = attrs.getNamedItem("uri");
                final String uri = getTextContent(uriNode);

                builder.addConstructorArgValue(uri);
                builder.addConstructorArgReference(instanceRef);
                builder.setFactoryMethod("getCacheManager");
            }
        }
    }
}
