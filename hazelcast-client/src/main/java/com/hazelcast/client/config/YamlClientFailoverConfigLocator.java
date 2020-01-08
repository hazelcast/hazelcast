/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
 * A support class for the {@link YamlClientFailoverConfigBuilder} to
 * locate the client failover YAML configuration.
 */
public class YamlClientFailoverConfigLocator extends AbstractConfigLocator {

    public YamlClientFailoverConfigLocator() {
        super(false);
    }

    public YamlClientFailoverConfigLocator(boolean failIfSysPropWithNotExpectedSuffix) {
        super(failIfSysPropWithNotExpectedSuffix);
    }

    @Override
    public boolean locateFromSystemProperty() {
        return loadFromSystemProperty("hazelcast.client.failover.config", "yaml", "yml");
    }

    @Override
    protected boolean locateInWorkDir() {
        return loadFromWorkingDirectory("hazelcast-client-failover.yaml");
    }

    @Override
    protected boolean locateOnClasspath() {
        return loadConfigurationFromClasspath("hazelcast-client-failover.yaml");
    }

    @Override
    public boolean locateDefault() {
        return false;
    }
}
