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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.spring.config.ConfigFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.rootBeanDefinition;

/**
 * BeanDefinitionParser for Hazelcast Client Configuration.
 * <pre>{@code
 *     <hz:client-failover id="blueGreenClient" try-count="5">
 *         <hz:client>
 *             <hz:cluster-name>${cluster.name}</hz:cluster-name>
 *             <hz:network>
 *                 <hz:member>127.0.0.1:5700</hz:member>
 *                 <hz:member>127.0.0.1:5701</hz:member>
 *             </hz:network>
 *         </hz:client>
 *
 *         <hz:client>
 *             <hz:cluster-name>alternativeClusterName</hz:cluster-name>
 *             <hz:network>
 *                 <hz:member>127.0.0.1:5702</hz:member>
 *                 <hz:member>127.0.0.1:5703</hz:member>
 *             </hz:network>
 *         </hz:client>
 *
 *     </hz:client-failover>
 * }</pre>
 */
public class HazelcastFailoverClientBeanDefinitionParser extends  AbstractHazelcastBeanDefinitionParser {

    @Override
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(parserContext);
        return springXmlBuilder.handleMultipleClusterAwareClient(element);
    }

    class SpringXmlBuilder extends SpringXmlBuilderHelper {

        private final ParserContext parserContext;
        private final BeanDefinitionBuilder builder;
        private final  BeanDefinitionBuilder failoverConfigBuilder;


        SpringXmlBuilder(ParserContext parserContext) {
            this(parserContext, rootBeanDefinition(HazelcastClient.class)
                    .setFactoryMethod("newHazelcastFailoverClient")
                    .setDestroyMethodName("shutdown"));
        }

        SpringXmlBuilder(ParserContext parserContext, BeanDefinitionBuilder builder) {
            this.parserContext = parserContext;
            this.builder = builder;
            this.failoverConfigBuilder = rootBeanDefinition(ConfigFactory.class, "newClientFailoverConfig");
        }

        AbstractBeanDefinition handleMultipleClusterAwareClient(Element element) {
            handleCommonBeanAttributes(element, builder, parserContext);
            String attribute = element.getAttribute("try-count");
            failoverConfigBuilder.addPropertyValue("tryCount", attribute);

            ManagedList<BeanDefinition> configs = new ManagedList<>();
            for (Node node : childElements(element)) {
                HazelcastClientBeanDefinitionParser.SpringXmlBuilder springXmlBuilder =
                        new HazelcastClientBeanDefinitionParser.SpringXmlBuilder(parserContext);
                configs.add(springXmlBuilder.createConfigBean(node));
            }

            failoverConfigBuilder.addPropertyValue("clientConfigs", configs);

            builder.addConstructorArgValue(failoverConfigBuilder.getBeanDefinition());
            return builder.getBeanDefinition();

        }
    }
}
