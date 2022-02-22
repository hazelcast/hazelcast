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

import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;

/**
 * BeanDefinitionParser for Hazelcast Instance Configuration.
 * <p>
 * <b>Sample Spring XML for Hazelcast Instance:</b>
 * <pre>{@code
 * <hz:hazelcast id="instance">
 *      <hz:config>
 *          <hz:cluster-name>dev</hz:cluster-name>
 *          <hz:network port="5701" port-auto-increment="false">
 *              <hz:join>
 *                  <hz:multicast enabled="false" multicast-group="224.2.2.3" multicast-port="54327"/>
 *                   <hz:tcp-ip enabled="true">
 *                      <hz:members>10.10.1.2, 10.10.1.3</hz:members>
 *                   </hz:tcp-ip>
 *              </hz:join>
 *          </hz:network>
 *          <hz:map name="map" backup-count="2" max-size="0"
 *             read-backup-data="true" eviction-policy="NONE"
 *          merge-policy="com.hazelcast.spi.merge.PassThroughMergePolicy"/>
 *      </hz:config>
 * </hz:hazelcast>
 * }</pre>
 */
public class HazelcastInstanceDefinitionParser extends AbstractHazelcastBeanDefinitionParser {

    static final String CP_SUBSYSTEM_SUFFIX = "@cp-subsystem";

    @Override
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        SpringXmlBuilder springXmlBuilder = new SpringXmlBuilder(parserContext);
        springXmlBuilder.handle(element);
        return springXmlBuilder.getBeanDefinition();
    }

    private class SpringXmlBuilder extends SpringXmlBuilderHelper {

        private final ParserContext parserContext;
        private final BeanDefinitionBuilder builder;

        SpringXmlBuilder(ParserContext parserContext) {
            this.parserContext = parserContext;
            this.builder = BeanDefinitionBuilder.rootBeanDefinition(HazelcastInstanceFactory.class);
            this.builder.setFactoryMethod("newHazelcastInstance");
            this.builder.setDestroyMethodName("shutdown");
        }

        AbstractBeanDefinition getBeanDefinition() {
            return builder.getBeanDefinition();
        }

        public void handle(Element element) {
            handleCommonBeanAttributes(element, builder, parserContext);
            Element config = null;
            for (Node node : childElements(element)) {
                String nodeName = cleanNodeName(node);
                if ("config".equals(nodeName)) {
                    config = (Element) node;
                }
            }
            HazelcastConfigBeanDefinitionParser configParser = new HazelcastConfigBeanDefinitionParser();
            AbstractBeanDefinition configBeanDef = configParser.parseInternal(config, parserContext);
            builder.addConstructorArgValue(configBeanDef);

            registerCPSubsystemBean(element);
        }

        private void registerCPSubsystemBean(Element element) {
            String instanceBeanRef = element.getAttribute("id");
            BeanDefinitionBuilder cpBeanDefBuilder = BeanDefinitionBuilder.rootBeanDefinition(CPSubsystem.class);
            cpBeanDefBuilder.setFactoryMethodOnBean("getCPSubsystem", instanceBeanRef);

            BeanDefinitionHolder holder =
                    new BeanDefinitionHolder(cpBeanDefBuilder.getBeanDefinition(), instanceBeanRef + CP_SUBSYSTEM_SUFFIX);
            registerBeanDefinition(holder, parserContext.getRegistry());
        }
    }
}
