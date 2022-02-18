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

package com.hazelcast.internal.config;

import com.hazelcast.config.replacer.spi.ConfigReplacer;
import com.hazelcast.internal.config.yaml.YamlElementAdapter;
import com.hazelcast.internal.yaml.MutableYamlNode;
import org.w3c.dom.Node;

import static com.hazelcast.internal.config.yaml.W3cDomUtil.getWrappedMutableYamlNode;

public class YamlDomVariableReplacer extends AbstractDomVariableReplacer {

    @Override
    public void replaceVariables(Node node, ConfigReplacer replacer, boolean failFast) {
        replaceVariableInNodeValue(node, replacer, failFast);
        replaceVariableInNodeName(node, replacer, failFast);
    }

    private void replaceVariableInNodeName(Node node, ConfigReplacer replacer, boolean failFast) {
        if (node instanceof YamlElementAdapter) {
            MutableYamlNode yamlNode = getWrappedMutableYamlNode(node);
            String nodeName = yamlNode.nodeName();
            if (nodeName != null) {
                String replacedName = replaceValue(node, replacer, failFast, nodeName);
                yamlNode.setNodeName(replacedName);
            }
        }
    }
}
