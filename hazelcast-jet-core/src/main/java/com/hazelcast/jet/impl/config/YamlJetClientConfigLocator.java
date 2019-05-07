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

package com.hazelcast.jet.impl.config;

import com.hazelcast.config.AbstractConfigLocator;

/**
 * A support class for the {@link YamlJetConfigBuilder} to locate the
 * client yaml configuration.
 */
public final class YamlJetClientConfigLocator extends AbstractConfigLocator {

    private static final String HAZELCAST_CLIENT_CONFIG_PROPERTY = "hazelcast.client.config";
    private static final String HAZELCAST_CLIENT_YAML = "hazelcast-client.yaml";
    private static final String HAZELCAST_CLIENT_DEFAULT_YAML = "hazelcast-jet-client-default.yaml";

    public YamlJetClientConfigLocator() {
        this(false);
    }

    public YamlJetClientConfigLocator(boolean failIfSysPropWithNotExpectedSuffix) {
        super(failIfSysPropWithNotExpectedSuffix);
    }

    @Override
    public boolean locateFromSystemProperty() {
        return loadFromSystemProperty(HAZELCAST_CLIENT_CONFIG_PROPERTY, "yaml", "yml");
    }

    @Override
    protected boolean locateInWorkDir() {
        return loadFromWorkingDirectory(HAZELCAST_CLIENT_YAML);
    }

    @Override
    protected boolean locateOnClasspath() {
        return loadConfigurationFromClasspath(HAZELCAST_CLIENT_YAML);
    }

    @Override
    public boolean locateDefault() {
        loadDefaultConfigurationFromClasspath(HAZELCAST_CLIENT_DEFAULT_YAML);
        return true;
    }
}
