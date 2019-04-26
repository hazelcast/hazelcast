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

package com.hazelcast.config;

/**
 * Support class for the {@link com.hazelcast.config.YamlConfigBuilder} that locates the YAML configuration:
 * <ol>
 * <li>system property</li>
 * <li>working directory</li>
 * <li>classpath</li>
 * <li>default</li>
 * </ol>
 */
public class YamlConfigLocator extends AbstractConfigLocator {

    public YamlConfigLocator() {
        this(false);
    }

    public YamlConfigLocator(boolean failIfSysPropWithNotExpectedSuffix) {
        super(failIfSysPropWithNotExpectedSuffix);
    }

    @Override
    public boolean locateFromSystemProperty() {
        return loadFromSystemProperty("hazelcast.config", "yaml", "yml");
    }

    @Override
    protected boolean locateInWorkDir() {
        return loadFromWorkingDirectory("hazelcast.yaml");
    }

    @Override
    protected boolean locateOnClasspath() {
        return loadConfigurationFromClasspath("hazelcast.yaml");
    }

    @Override
    public boolean locateDefault() {
        loadDefaultConfigurationFromClasspath("hazelcast-default.yaml");
        return true;
    }
}
