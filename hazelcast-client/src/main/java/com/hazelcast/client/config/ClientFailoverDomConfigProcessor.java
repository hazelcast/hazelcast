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

package com.hazelcast.client.config;

import com.hazelcast.config.AbstractDomConfigProcessor;
import com.hazelcast.config.InvalidConfigurationException;
import org.w3c.dom.Node;

import java.io.IOException;

import static com.hazelcast.client.config.ClientFailoverConfigSections.CLIENTS;
import static com.hazelcast.client.config.ClientFailoverConfigSections.TRY_COUNT;
import static com.hazelcast.client.config.ClientFailoverConfigSections.canOccurMultipleTimes;
import static com.hazelcast.config.DomConfigHelper.childElements;
import static com.hazelcast.config.DomConfigHelper.cleanNodeName;


class ClientFailoverDomConfigProcessor extends AbstractDomConfigProcessor {

    protected final ClientFailoverConfig clientFailoverConfig;

    ClientFailoverDomConfigProcessor(boolean domLevel3, ClientFailoverConfig clientFailoverConfig) {
        super(domLevel3);
        this.clientFailoverConfig = clientFailoverConfig;
    }

    @Override
    public void buildConfig(Node rootNode) {
        for (Node node : childElements(rootNode)) {
            String nodeName = cleanNodeName(node);
            if (occurrenceSet.contains(nodeName)) {
                throw new InvalidConfigurationException("Duplicate '" + nodeName + "' definition found in the configuration");
            }
            handleNode(node, nodeName);
            if (!canOccurMultipleTimes(nodeName)) {
                occurrenceSet.add(nodeName);
            }
        }
    }

    private void handleNode(Node node, String nodeName) {
        if (CLIENTS.isEqual(nodeName)) {
            handleClients(node);
        } else if (TRY_COUNT.isEqual(nodeName)) {
            handleTryCount(node);
        }
    }

    protected void handleClients(Node node) {
        for (Node child : childElements(node)) {
            if ("client".equals(cleanNodeName(child))) {
                String clientPath = getTextContent(child);
                try {
                    ClientConfig config = new XmlClientConfigBuilder(clientPath).build();
                    clientFailoverConfig.addClientConfig(config);
                } catch (IOException e) {
                    throw new InvalidConfigurationException("Could not create the config from given path : " + clientPath, e);
                }
            }
        }
    }

    private void handleTryCount(Node node) {
        int tryCount = Integer.parseInt(getTextContent(node));
        clientFailoverConfig.setTryCount(tryCount);
    }
}
