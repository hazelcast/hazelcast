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

import com.hazelcast.config.AbstractConfigLocator;
import com.hazelcast.core.HazelcastException;

/**
 * A support class for the {@link XmlClientConfigBuilder} to locate the client
 * xml configuration.
 */
public class XmlClientConfigLocator extends AbstractConfigLocator {
    /**
     * Constructs a XmlClientConfigBuilder.
     *
     * @throws com.hazelcast.core.HazelcastException if the client XML config is not located.
     */
    public XmlClientConfigLocator() {
        try {
            if (loadFromSystemProperty("hazelcast.client.config")) {
                return;
            }

            if (loadFromWorkingDirectory("hazelcast-client.xml")) {
                return;
            }

            if (loadHazelcastConfigFromClasspath("hazelcast-client.xml")) {
                return;
            }

            loadDefaultConfigurationFromClasspath("hazelcast-client-default.xml");
        } catch (final RuntimeException e) {
            throw new HazelcastException("Failed to load ClientConfig", e);
        }
    }
}
