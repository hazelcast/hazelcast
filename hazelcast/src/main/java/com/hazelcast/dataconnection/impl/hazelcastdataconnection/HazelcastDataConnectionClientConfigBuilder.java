/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dataconnection.impl.hazelcastdataconnection;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.jet.impl.util.ImdgUtil;

import static com.hazelcast.dataconnection.HazelcastDataConnection.CLIENT_XML;
import static com.hazelcast.dataconnection.HazelcastDataConnection.CLIENT_YML;

public class HazelcastDataConnectionClientConfigBuilder {

    /**
     * Converts the string in the DataConnectionConfig to ClientConfig object
     */
    public ClientConfig buildClientConfig(DataConnectionConfig dataConnectionConfig) {

        String clientXml = dataConnectionConfig.getProperty(CLIENT_XML);
        if (!StringUtil.isNullOrEmpty(clientXml)) {
            // Read ClientConfig from XML
            return ImdgUtil.asClientConfig(clientXml);
        }

        String clientYaml = dataConnectionConfig.getProperty(CLIENT_YML);
        if (!StringUtil.isNullOrEmpty(clientYaml)) {
            // Read ClientConfig from Yaml
            return ImdgUtil.asClientConfigFromYaml(clientYaml);
        }
        throw new HazelcastException("Invalid state. Both XML and YML strings are empty. HazelcastDataConnection with name '"
                                     + dataConnectionConfig.getName()
                                     + "' could not be created");
    }
}
