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

package com.hazelcast.client.config.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFailoverConfig;
import com.hazelcast.client.config.YamlClientConfigBuilder;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.config.yaml.YamlElementAdapter;
import org.w3c.dom.Node;

import java.io.IOException;

import static com.hazelcast.internal.config.DomConfigHelper.childElements;

public class YamlClientFailoverDomConfigProcessor extends ClientFailoverDomConfigProcessor {
    public YamlClientFailoverDomConfigProcessor(boolean domLevel3, ClientFailoverConfig clientFailoverConfig) {
        super(domLevel3, clientFailoverConfig);
    }

    @Override
    protected void handleClients(Node node) {
        boolean clientConfigDefined = false;

        for (Node child : childElements(node)) {
            String clientPath = getTextContent(child);
            try {
                ClientConfig config = new YamlClientConfigBuilder(clientPath).build();
                clientFailoverConfig.addClientConfig(config);
                clientConfigDefined = true;
            } catch (IOException e) {
                throw new InvalidConfigurationException("Could not create the config from given path : " + clientPath, e);
            }
        }

        if (!clientConfigDefined) {
            String path = ((YamlElementAdapter) node).getYamlNode().path();
            throw new InvalidConfigurationException(String.format("At least one client configuration must be defined "
                    + "under '%s'", path));
        }
    }
}
