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

import com.hazelcast.config.AbstractXmlConfigHelper;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 * @mdogan 4/10/12
 */
public abstract class AbstractHazelcastBeanDefinitionParser extends AbstractBeanDefinitionParser {

    public abstract class SpringXmlBuilderHelper extends AbstractXmlConfigHelper {

        protected void handleCommonBeanAttributes(Node node, BeanDefinitionBuilder builder, ParserContext parserContext) {
            final NamedNodeMap attributes = node.getAttributes();
            if (attributes != null) {
                Node lazyInitAttr = attributes.getNamedItem("lazy-init");
                if (lazyInitAttr != null) {
                    builder.setLazyInit(Boolean.valueOf(getTextContent(lazyInitAttr)));
                } else {
                    builder.setLazyInit(parserContext.isDefaultLazyInit());
                }

                if (parserContext.isNested()) {
                    builder.setScope(parserContext.getContainingBeanDefinition().getScope());
                } else {
                    Node scopeNode = attributes.getNamedItem("scope");
                    if (scopeNode != null) {
                        builder.setScope(getTextContent(scopeNode));
                    }
                }

                Node dependsOnNode = attributes.getNamedItem("depends-on");
                if (dependsOnNode != null) {
                    String[] dependsOn = getTextContent(dependsOnNode).split("[,;]");
                    for (String dep : dependsOn) {
                        builder.addDependsOn(dep.trim());
                    }
                }
            }
        }
    }
}
