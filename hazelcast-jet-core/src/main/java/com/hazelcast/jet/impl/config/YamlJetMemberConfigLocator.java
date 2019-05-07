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
 * A support class for the {@link XmlJetConfigBuilder} to locate the member
 * yaml configuration.
 */
public final class YamlJetMemberConfigLocator extends AbstractConfigLocator {

    private static final String HAZELCAST_MEMBER_CONFIG_PROPERTY = "hazelcast.config";
    private static final String HAZELCAST_MEMBER_YAML = "hazelcast.yaml";
    private static final String HAZELCAST_MEMBER_DEFAULT_YAML = "hazelcast-jet-member-default.yaml";
    private static final String HAZELCAST_ENTERPRISE_MEMBER_DEFAULT_YAML = "hazelcast-jet-enterprise-member-default.yaml";

    public YamlJetMemberConfigLocator() {
        super(true);
    }

    @Override
    public boolean locateFromSystemProperty() {
        return loadFromSystemProperty(HAZELCAST_MEMBER_CONFIG_PROPERTY, "yaml", "yml");
    }

    @Override
    protected boolean locateInWorkDir() {
        return loadFromWorkingDirectory(HAZELCAST_MEMBER_YAML);
    }

    @Override
    protected boolean locateOnClasspath() {
        return loadConfigurationFromClasspath(HAZELCAST_MEMBER_YAML)
                || loadConfigurationFromClasspath(HAZELCAST_ENTERPRISE_MEMBER_DEFAULT_YAML);
    }

    @Override
    public boolean locateDefault() {
        loadDefaultConfigurationFromClasspath(HAZELCAST_MEMBER_DEFAULT_YAML);
        return true;
    }
}
