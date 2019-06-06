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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.w3c.dom.Node;

import static java.lang.String.format;

abstract class AbstractDomVariableReplacer implements DomVariableReplacer {
    private static final ILogger LOGGER = Logger.getLogger(ConfigReplacerHelper.class);

    protected static String replaceValue(Node node, ConfigReplacer replacer, boolean failFast, String value) {
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

    void replaceVariableInNodeValue(Node node, ConfigReplacer replacer, boolean failFast) {
        String value = node.getNodeValue();
        if (value != null) {
            String replacedValue = replaceValue(node, replacer, failFast, value);
            node.setNodeValue(replacedValue);
        }
    }

    private static void handleMissingVariable(String variable, String nodeName, boolean failFast)
            throws InvalidConfigurationException {
        String message = format("Could not find a replacement for '%s' on node '%s'", variable, nodeName);
        if (failFast) {
            throw new InvalidConfigurationException(message);
        }
        LOGGER.warning(message);
    }
}
