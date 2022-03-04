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

import com.hazelcast.client.config.YamlClientConfigBuilder;
import com.hazelcast.internal.config.AbstractConfigLocator;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.SYSPROP_CLIENT_CONFIG;
import static com.hazelcast.internal.config.DeclarativeConfigUtil.YAML_ACCEPTED_SUFFIXES;

/**
 * A support class for the {@link YamlClientConfigBuilder} to locate the client
 * YAML configuration.
 */
public class YamlClientConfigLocator extends AbstractConfigLocator {

    @Override
    public boolean locateFromSystemProperty() {
        return loadFromSystemProperty(SYSPROP_CLIENT_CONFIG, YAML_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateFromSystemPropertyOrFailOnUnacceptedSuffix() {
        return loadFromSystemPropertyOrFailOnUnacceptedSuffix(SYSPROP_CLIENT_CONFIG, YAML_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateInWorkDir() {
        return loadFromWorkingDirectory("hazelcast-client", YAML_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateOnClasspath() {
        return loadConfigurationFromClasspath("hazelcast-client", YAML_ACCEPTED_SUFFIXES);
    }

    @Override
    public boolean locateDefault() {
        loadDefaultConfigurationFromClasspath("hazelcast-client-default.yaml");
        return true;
    }
}
