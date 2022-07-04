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

import com.hazelcast.config.AbstractXmlConfigHelper;
import com.hazelcast.internal.services.ServiceConfigurationParser;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;
import static com.hazelcast.internal.config.DomConfigHelper.cleanNodeName;
import static com.hazelcast.internal.config.DomConfigHelper.getTextContent;

public class MyServiceConfigParser extends AbstractXmlConfigHelper implements ServiceConfigurationParser<MyServiceConfig> {

    public MyServiceConfig parse(Element element) {
        MyServiceConfig config = new MyServiceConfig();
        for (Node configNode : childElements(element)) {
            if ("my-service".equals(cleanNodeName(configNode))) {
                for (Node node : childElements(configNode)) {
                    String name = cleanNodeName(node);
                    if ("string-prop".equals(name)) {
                        config.stringProp = getTextContent(node, domLevel3);
                    } else if ("int-prop".equals(name)) {
                        String value = getTextContent(node, domLevel3);
                        config.intProp = Integer.parseInt(value);
                    } else if ("bool-prop".equals(name)) {
                        config.boolProp = Boolean.parseBoolean(getTextContent(node, domLevel3));
                    }
                }
            }
        }
        return config;
    }
}
