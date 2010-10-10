/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.spring;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.config.XmlConfigBuilder.IterableNodeList;

public class HazelcastBeanDefinitionParser extends AbstractSimpleBeanDefinitionParser {

	@Override
    protected Class<Config> getBeanClass(Element element) {
        return Config.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        super.doParse(element, parserContext, builder);
        
        XmlConfigBuilder util = new XmlConfigBuilder();
        Config config = util.build();
        try {
            for (org.w3c.dom.Node node : new IterableNodeList(element.getChildNodes())) {
                final String nodeName = util.cleanNodeName(node.getNodeName());
               
                if ("network".equals(nodeName)) {
                    util.handleNetwork(node);
                } else if ("group".equals(nodeName)) {
                    util.handleGroup(node);
                } else if ("properties".equals(nodeName)) {
                    util.handleProperties(node, config.getProperties());
                } else if ("executor-service".equals(nodeName)) {
                    util.handleExecutor(node);
                } else if ("queue".equals(nodeName)) {
                    util.handleQueue(node);
                } else if ("map".equals(nodeName)) {
                    util.handleMap(node);
                } else if ("topic".equals(nodeName)) {
                    util.handleTopic(node);
                } else if ("merge-policies".equals(nodeName)) {
                    util.handleMergePolicies(node);
                }
            }

        } catch (Exception e) {
            throw new IllegalStateException("Configuration failed due to:" + e.getMessage(), e);
        }

        builder.addPropertyValue("networkConfig", config.getNetworkConfig());
        builder.addPropertyValue("groupConfig", config.getGroupConfig());
        builder.addPropertyValue("properties", config.getProperties());
        builder.addPropertyValue("executorConfigMap", config.getExecutorConfigMap());
        builder.addPropertyValue("mapConfigs", config.getMapConfigs());
        builder.addPropertyValue("qConfigs", config.getQConfigs());
        
    }

}
