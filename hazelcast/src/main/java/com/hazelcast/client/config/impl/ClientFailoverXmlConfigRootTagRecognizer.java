/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.ConfigRecognizer;
import com.hazelcast.internal.config.AbstractXmlConfigRootTagRecognizer;

/**
 * This {@link ConfigRecognizer} implementation recognizes Hazelcast
 * failover client XML configuration by checking if the defined root tag
 * is "hazelcast-client-failover" or not. For the implementation details
 * please refer to the {@link AbstractXmlConfigRootTagRecognizer}
 * documentation.
 */
public class ClientFailoverXmlConfigRootTagRecognizer extends AbstractXmlConfigRootTagRecognizer {
    public ClientFailoverXmlConfigRootTagRecognizer() throws Exception {
        super(ClientFailoverConfigSections.CLIENT_FAILOVER.getName());
    }
}
