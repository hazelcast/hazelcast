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
import com.hazelcast.spring.HazelcastClientBeanDefinitionParser;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

import static org.springframework.beans.factory.support.BeanDefinitionBuilder.rootBeanDefinition;

/**
 * BeanDefinitionParser for Hazelcast Jet Client Configuration.
 * <p>
 * <b>Sample Spring XML for Hazelcast Jet Client:</b>
 * <pre>{@code
 *   <jet:client id="client">
 *      <jet:group name="jet" password="jet-pass"/>
 *      <jet:network connection-attempt-limit="3"
 *          connection-attempt-period="3000"
 *          connection-timeout="1000"
 *          redo-operation="true"
 *          smart-routing="true">
 *              <hz:member>10.10.1.2:5701</hz:member>
 *              <hz:member>10.10.1.3:5701</hz:member>
 *      </jet:network>
 *   </jet:client>
 * }</pre>
 */
public class JetClientBeanDefinitionParser extends HazelcastClientBeanDefinitionParser {
    @Override
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
        JetClientSpringXmlBuilder springXmlBuilder = new JetClientSpringXmlBuilder(parserContext);
        return springXmlBuilder.handleClient(element);
    }

    private class JetClientSpringXmlBuilder extends HazelcastClientBeanDefinitionParser.SpringXmlBuilder {

        JetClientSpringXmlBuilder(ParserContext parserContext) {
            super(parserContext, rootBeanDefinition(Jet.class)
                    .setFactoryMethod("newJetClient")
                    .setDestroyMethodName("shutdown"));
        }

    }
}
