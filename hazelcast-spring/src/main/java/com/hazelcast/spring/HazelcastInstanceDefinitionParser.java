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

import com.hazelcast.instance.HazelcastInstanceFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * BeanDefinitionParser for Hazelcast Instance Configuration
 * <p/>
 *
 * <b>Sample Spring XML for Hazelcast Instance:</b>
 * <pre>
 * &lt;hz:hazelcast id="instance"&gt;
 *      &lt;hz:config&gt;
 *          &lt;hz:group name="dev" password="password"/&gt;
 *          &lt;hz:network port="5701" port-auto-increment="false"&gt;
 *              &lt;hz:join&gt;
 *                  &lt;hz:multicast enabled="false" multicast-group="224.2.2.3" multicast-port="54327"/&gt;
 *                   &lt;hz:tcp-ip enabled="true"&gt;
 *                      &lt;hz:members&gt;10.10.1.2, 10.10.1.3&lt;/hz:members&gt;
 *                   &lt;/hz:tcp-ip&gt;
 *              &lt;/hz:join&gt;
 *          &lt;/hz:network&gt;
 *          &lt;hz:map name="map" backup-count="2" max-size="0" eviction-percentage="30"
 *              read-backup-data="true" eviction-policy="NONE"
 *          merge-policy="com.hazelcast.map.merge.PassThroughMergePolicy"/&gt;
 *      &lt;/hz:config&gt;
 * &lt;/hz:hazelcast&gt;
 * </pre>
 */
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
            this.builder = BeanDefinitionBuilder.rootBeanDefinition(HazelcastInstanceFactory.class);
            this.builder.setFactoryMethod("newHazelcastInstance");
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
