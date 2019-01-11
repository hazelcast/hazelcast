/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.MetricsConfig;
import com.hazelcast.spring.AbstractHazelcastBeanDefinitionParser;
import com.hazelcast.spring.HazelcastConfigBeanDefinitionParser;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import static com.hazelcast.config.DomConfigHelper.childElements;
import static com.hazelcast.config.DomConfigHelper.cleanNodeName;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.rootBeanDefinition;

/**
 * BeanDefinitionParser for Hazelcast Jet Instance Configuration.
 * <p>
 * <b>Sample Spring XML for Hazelcast Jet Instance:</b>
 * <pre>{@code
 * <jet:instance id="jet-instance">
 *      <hz:config>
 *          <hz:spring-aware/>
 *          <hz:group name="jet" password="jet-pass"/>
 *          <hz:network port="5701" port-auto-increment="false">
 *              <hz:join>
 *                  <hz:multicast enabled="false"/>
 *                   <hz:tcp-ip enabled="true">
 *                      <hz:members>10.10.1.2, 10.10.1.3</hz:members>
 *                   </hz:tcp-ip>
 *              </hz:join>
 *          </hz:network>
 *          <hz:map name="map" backup-count="2" max-size="0" eviction-percentage="30"
 *              read-backup-data="true" eviction-policy="NONE"
 *          merge-policy="com.hazelcast.map.merge.PassThroughMergePolicy"/>
 *      </hz:config>
 * </jet:instance>
 * }</pre>
 */
public class JetInstanceBeanDefinitionParser extends AbstractHazelcastBeanDefinitionParser {

    @Override
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(parserContext);
        return springXmlBuilder.handle(element);
    }

    private class SpringXmlBuilder extends SpringXmlBuilderHelper {

        private final ParserContext parserContext;
        private final BeanDefinitionBuilder builder;

        SpringXmlBuilder(ParserContext parserContext) {
            this.parserContext = parserContext;
            this.builder = rootBeanDefinition(Jet.class)
                    .setFactoryMethod("newJetInstance")
                    .setDestroyMethodName("shutdown");
        }

        private AbstractBeanDefinition handle(Element element) {
            handleCommonBeanAttributes(element, builder, parserContext);
            configBuilder = rootBeanDefinition(JetConfig.class)
                    .setScope(builder.getBeanDefinition().getScope())
                    .setLazyInit(builder.getBeanDefinition().isLazyInit());

            for (Node node : childElements(element)) {
                String nodeName = cleanNodeName(node);
                if ("config".equals(nodeName)) {
                    configBuilder.addPropertyValue("hazelcastConfig", parseConfigBeanDefinition((Element) node));
                } else if ("instance-config".equals(nodeName)) {
                    createAndFillBeanBuilder(node, InstanceConfig.class, "instanceConfig", configBuilder);
                } else if ("default-edge-config".equals(nodeName)) {
                    createAndFillBeanBuilder(node, EdgeConfig.class, "defaultEdgeConfig", configBuilder);
                } else if ("properties".equals(nodeName)) {
                    handleProperties(node, configBuilder);
                } else if ("metrics-config".equals(nodeName)) {
                    createAndFillBeanBuilder(node, MetricsConfig.class, "metricsConfig", configBuilder);
                } else {
                    throw new IllegalArgumentException("Unknown configuration for JetConfig nodeName: " + nodeName);
                }
            }

            return builder.addConstructorArgValue(configBuilder.getBeanDefinition())
                          .getBeanDefinition();
        }

        private AbstractBeanDefinition parseConfigBeanDefinition(Element config) {
            if (config == null) {
                return defaultConfigBeanDefinition();
            }
            HazelcastConfigBeanDefinitionParser configParser = new HazelcastConfigBeanDefinitionParser();
            return configParser.parseInternal(config, parserContext);
        }

        private AbstractBeanDefinition defaultConfigBeanDefinition() {
            return rootBeanDefinition(JetConfig.class)
                    .setFactoryMethod("defaultHazelcastConfig").getBeanDefinition();
        }
    }
}
