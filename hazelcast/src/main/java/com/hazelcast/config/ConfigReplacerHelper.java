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

package com.hazelcast.config;

import com.hazelcast.config.replacer.spi.ConfigReplacer;
import com.hazelcast.config.yaml.ElementAdapter;
import com.hazelcast.internal.yaml.MutableYamlNode;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.List;

import static java.lang.String.format;

/**
 * Helper class for replacing variables in the XML and YAML configuration DOMs
 */
final class ConfigReplacerHelper {

    private static final ILogger LOGGER = Logger.getLogger(ConfigReplacerHelper.class);

    private ConfigReplacerHelper() {
    }

    static void traverseChildrenAndReplaceVariables(Node root, List<ConfigReplacer> replacers, boolean failFast) {

        // Use all the replacers on the content
        for (ConfigReplacer replacer : replacers) {
            traverseChildrenAndReplaceVariables(root, replacer, failFast);
        }
    }

    private static void traverseChildrenAndReplaceVariables(Node root, ConfigReplacer replacer, boolean failFast) {
        NamedNodeMap attributes = root.getAttributes();
        if (attributes != null) {
            for (int k = 0; k < attributes.getLength(); k++) {
                Node attribute = attributes.item(k);
                replaceVariables(attribute, replacer, failFast);
            }
        }
        if (root.getNodeValue() != null) {
            replaceVariables(root, replacer, failFast);
        }
        final NodeList childNodes = root.getChildNodes();
        for (int k = 0; k < childNodes.getLength(); k++) {
            Node child = childNodes.item(k);
            if (child != null) {
                traverseChildrenAndReplaceVariables(child, replacer, failFast);
            }
        }
    }

    private static void replaceVariables(Node node, ConfigReplacer replacer, boolean failFast) {
        if (node == null) {
            return;
        }

        String value = node.getNodeValue();
        if (value != null) {
            String replacedValue = replaceValue(node, replacer, failFast, value);
            node.setNodeValue(replacedValue);
        }

        // if its an ElementAdapter, this is a YAML node, in which case
        // we may need to replace variable in the name of the node as well
        if (node instanceof ElementAdapter) {
            MutableYamlNode yamlNode = (MutableYamlNode) ((ElementAdapter) node).getYamlNode();
            String nodeName = yamlNode.nodeName();
            if (nodeName != null) {
                String replacedName = replaceValue(node, replacer, failFast, nodeName);
                yamlNode.setNodeName(replacedName);
            }
        }
    }

    private static String replaceValue(Node node, ConfigReplacer replacer, boolean failFast, String value) {
        StringBuilder sb = new StringBuilder(value);
        String replacerPrefix = "$" + replacer.getPrefix() + "{";
        int endIndex = -1;
        int startIndex = sb.indexOf(replacerPrefix);
        while (startIndex > -1) {
            endIndex = sb.indexOf("}", startIndex);
            if (endIndex == -1) {
                LOGGER.warning("Bad variable syntax. Could not find a closing curly bracket '}' for prefix " + replacerPrefix
                        + " on node: " + node.getLocalName());
                break;
            }

            String variable = sb.substring(startIndex + replacerPrefix.length(), endIndex);
            String variableReplacement = replacer.getReplacement(variable);
            if (variableReplacement != null) {
                sb.replace(startIndex, endIndex + 1, variableReplacement);
                endIndex = startIndex + variableReplacement.length();
            } else {
                handleMissingVariable(sb.substring(startIndex, endIndex + 1), node.getLocalName(), failFast);
            }
            startIndex = sb.indexOf(replacerPrefix, endIndex);
        }
        return sb.toString();
    }

    private static void handleMissingVariable(String variable, String nodeName, boolean failFast) throws ConfigurationException {
        String message = format("Could not find a replacement for '%s' on node '%s'", variable, nodeName);
        if (failFast) {
            throw new ConfigurationException(message);
        }
        LOGGER.warning(message);
    }
}
