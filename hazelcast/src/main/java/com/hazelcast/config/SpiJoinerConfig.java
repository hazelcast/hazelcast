/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import org.w3c.dom.Node;

/**
 * class to save information of the spi joiner configuration.
 */
public class SpiJoinerConfig {
    private boolean enabled;
    private String type;
    private Node xmlNode;

    public SpiJoinerConfig(boolean enabled, String type, Node xmlNode) {
        this.enabled = enabled;
        this.type = type;
        this.xmlNode = xmlNode;
    }

    public String getType() {
        return type;
    }

    public Node getXmlNode() {
        return xmlNode;
    }

    public boolean isEnabled() {
        return enabled;
    }
}
