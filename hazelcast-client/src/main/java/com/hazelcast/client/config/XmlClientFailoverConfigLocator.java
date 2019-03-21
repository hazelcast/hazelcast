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

/**
 * A support class for the {@link XmlClientFailoverConfigBuilder} to
 * locate the client failover XML configuration.
 */
public class XmlClientFailoverConfigLocator extends AbstractConfigLocator {

    @Override
    public boolean locateFromSystemProperty() {
        return loadFromSystemProperty("hazelcast.client.failover.config", "xml");
    }

    @Override
    protected boolean locateInWorkDir() {
        return loadFromWorkingDirectory("hazelcast-client-failover.xml");
    }

    @Override
    protected boolean locateOnClasspath() {
        return loadConfigurationFromClasspath("hazelcast-client-failover.xml");
    }

    @Override
    public boolean locateDefault() {
        return false;
    }
}
