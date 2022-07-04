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
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.List;

/**
 * Helper class for replacing variables in the configuration DOMs.
 */
public final class ConfigReplacerHelper {

    private ConfigReplacerHelper() {
    }

    public static void traverseChildrenAndReplaceVariables(Node root, List<ConfigReplacer> replacers, boolean failFast,
                                                    DomVariableReplacer variableReplacer) {
        // Use all the replacers on the content
        for (ConfigReplacer replacer : replacers) {
            traverseChildrenAndReplaceVariables(root, replacer, failFast, variableReplacer);
        }
    }

    private static void traverseChildrenAndReplaceVariables(Node root, ConfigReplacer replacer, boolean failFast,
                                                            DomVariableReplacer variableReplacer) {
        if (root == null) {
            return;
        }
        NamedNodeMap attributes = root.getAttributes();
        if (attributes != null) {
            for (int k = 0; k < attributes.getLength(); k++) {
                Node attribute = attributes.item(k);
                variableReplacer.replaceVariables(attribute, replacer, failFast);
            }
        }
        if (root.getNodeValue() != null) {
            variableReplacer.replaceVariables(root, replacer, failFast);
        }
        final NodeList childNodes = root.getChildNodes();
        for (int k = 0; k < childNodes.getLength(); k++) {
            Node child = childNodes.item(k);
            traverseChildrenAndReplaceVariables(child, replacer, failFast, variableReplacer);
        }
    }

}
