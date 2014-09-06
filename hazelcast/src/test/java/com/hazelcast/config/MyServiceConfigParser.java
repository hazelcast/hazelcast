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

package com.hazelcast.config;

import com.hazelcast.spi.ServiceConfigurationParser;
import org.w3c.dom.Element;
public class MyServiceConfigParser extends AbstractXmlConfigHelper implements ServiceConfigurationParser<MyServiceConfig> {

    public MyServiceConfig parse(Element element) {
        MyServiceConfig config = new MyServiceConfig();
        for (org.w3c.dom.Node configNode : new AbstractXmlConfigHelper.IterableNodeList(element.getChildNodes())) {
            if ("my-service".equals(cleanNodeName(configNode))) {
                for (org.w3c.dom.Node node : new AbstractXmlConfigHelper.IterableNodeList(configNode.getChildNodes())) {
                    final String name = cleanNodeName(node.getNodeName());
                    if ("string-prop".equals(name)) {
                        config.stringProp = getTextContent(node);
                    } else if ("int-prop".equals(name)) {
                        String value = getTextContent(node);
                        config.intProp = Integer.parseInt(value);
                    } else if ("bool-prop".equals(name)) {
                        config.boolProp = Boolean.parseBoolean(getTextContent(node));
                    }
                }
            }
        }
        return config;
    }

}
