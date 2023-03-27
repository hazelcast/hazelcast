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

package com.hazelcast.datalink.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.datalink.HazelcastDataLink;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.jet.impl.util.ImdgUtil;

public class HazelcastDataLinkClientConfigBuilder {

    public ClientConfig buildClientConfig(DataLinkConfig dataLinkConfig) {

        String clientXml = dataLinkConfig.getProperty(HazelcastDataLink.CLIENT_XML);
        if (!StringUtil.isNullOrEmpty(clientXml)) {
            // Read ClientConfig from XML
            return ImdgUtil.asClientConfig(clientXml);
        }

        String clientYaml = dataLinkConfig.getProperty(HazelcastDataLink.CLIENT_YML);
        if (!StringUtil.isNullOrEmpty(clientYaml)) {
            // Read ClientConfig from Yaml
            return ImdgUtil.asClientConfigFromYaml(clientYaml);
        }
        throw new HazelcastException("HazelcastDataLink with name '" + dataLinkConfig.getName()
                                     + "' could not be created, "
                                     + "provide either a file path with client_xml_path or client_yaml_path property "
                                     + "or string content with client_xml or client_yml property "
                                     + "with the client configuration.");
    }
}
