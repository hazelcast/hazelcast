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

package com.hazelcast.spring.hibernate;

import com.hazelcast.hibernate.HazelcastCacheRegionFactory;
import com.hazelcast.hibernate.HazelcastLocalCacheRegionFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 * Parser for RegionFactory.
 * <p/>
 *
 * Sample Spring XML for Hibernate RegionFactory
 * <pre>
 * <code>
 *     &lt;hz:hibernate-region-factory id="regionFactory" instance-ref="instance"/&gt;
 *     &lt;hz:hibernate-region-factory id="localRegionFactory" instance-ref="instance" mode="LOCAL" /&gt;
 * </code>
 * </pre>
 *
 */
public class RegionFactoryBeanDefinitionParser extends AbstractBeanDefinitionParser {

    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        final NamedNodeMap atts = element.getAttributes();
        String instanceRefName = "instance";
        String mode = "DISTRIBUTED";
        if (atts != null) {
            for (int a = 0; a < atts.getLength(); a++) {
                final Node att = atts.item(a);
                final String name = att.getNodeName();
                if ("instance-ref".equals(name)) {
                    instanceRefName = att.getTextContent();
                } else if ("mode".equals(name)) {
                    mode = att.getTextContent();
                }
            }
        }

        final BeanDefinitionBuilder builder;
        if ("DISTRIBUTED".equals(mode)) {
            builder = BeanDefinitionBuilder.rootBeanDefinition(HazelcastCacheRegionFactory.class);
        } else if ("LOCAL".equals(mode)) {
            builder = BeanDefinitionBuilder.rootBeanDefinition(HazelcastLocalCacheRegionFactory.class);
        } else {
            throw new IllegalArgumentException("Unknown Hibernate L2 cache mode: " + mode);
        }
        builder.addConstructorArgReference(instanceRefName);
        return builder.getBeanDefinition();
    }
}
