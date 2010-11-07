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

public class HazelcastBeanDefinitionParser extends AbstractSimpleBeanDefinitionParser {

	@Override
    protected Class<Config> getBeanClass(Element element) {
        return Config.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        super.doParse(element, parserContext, builder);
        
        XmlConfigBuilder util = new XmlConfigBuilder();
        Config config = util.build(element);

        builder.addPropertyValue("port", config.getPort());
        builder.addPropertyValue("portAutoIncrement", config.isPortAutoIncrement());
        builder.addPropertyValue("reuseAddress", config.isReuseAddress());
        builder.addPropertyValue("superClient", config.isSuperClient());
        builder.addPropertyValue("networkConfig", config.getNetworkConfig());
        builder.addPropertyValue("groupConfig", config.getGroupConfig());
        builder.addPropertyValue("properties", config.getProperties());
        builder.addPropertyValue("executorConfigMap", config.getExecutorConfigMap());
        builder.addPropertyValue("mapConfigs", config.getMapConfigs());
        builder.addPropertyValue("QConfigs", config.getQConfigs());
        
    }

}
