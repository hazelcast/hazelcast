/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.config.processors;

import com.hazelcast.config.EntryListenerConfig;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import static com.hazelcast.internal.config.DomConfigHelper.getBooleanValue;
import static com.hazelcast.internal.config.DomConfigHelper.getTextContent;

class EntryListenerProcessor implements Processor<EntryListenerConfig> {
    private final Node node;
    private final boolean domLevel3;

    EntryListenerProcessor(Node node, boolean domLevel3) {
        this.node = node;
        this.domLevel3 = domLevel3;
    }

    @Override
    public EntryListenerConfig process() {
        NamedNodeMap attrs = node.getAttributes();
        boolean incValue = getBooleanValue(getTextContent(attrs.getNamedItem("include-value"), domLevel3));
        boolean local = getBooleanValue(getTextContent(attrs.getNamedItem("local"), domLevel3));
        String listenerClass = getTextContent(node, domLevel3);
        return new EntryListenerConfig(listenerClass, local, incValue);
    }
}
