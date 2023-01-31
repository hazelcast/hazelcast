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

package com.hazelcast.internal.config;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A ConfigNamespace that makes identification of config within a section possible.
 */
public class ConfigNamespace {
    private final ConfigSections configSection;
    private final String configName;

    public ConfigNamespace(@Nonnull ConfigSections configSection, @Nonnull String configName) {
        this.configSection = configSection;
        this.configName = configName;
    }

    public ConfigNamespace(@Nonnull ConfigSections configSection) {
        this.configSection = configSection;
        this.configName = null;
    }

    public ConfigSections getConfigSection() {
        return configSection;
    }

    public String getSectionName() {
        return configSection.getName();
    }

    public String getConfigName() {
        return configName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConfigNamespace that = (ConfigNamespace) o;
        return configSection == that.configSection && Objects.equals(configName, that.configName);
    }

    @Override
    public String toString() {
        return "ConfigNamespace{"
                + "configSection=" + configSection
                + ", configName='" + configName + '\''
                + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(configSection, configName);
    }
}
