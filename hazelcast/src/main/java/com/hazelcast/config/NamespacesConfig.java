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

package com.hazelcast.config;

import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serves as the configuration container for all aspects of Namespaces, used in User Code Deployment.
 * Individual Namespaces are defined within {@link NamespaceConfig}, and a class filtering config
 * can optionally be provided with an implementation of {@link JavaSerializationFilterConfig}.
 *
 * @since 5.4
 */
@Beta
public class NamespacesConfig {
    private boolean enabled;
    private final Map<String, NamespaceConfig> namespaceConfigs = new ConcurrentHashMap<>();
    private @Nullable JavaSerializationFilterConfig classFilterConfig;

    public NamespacesConfig() {
    }

    public NamespacesConfig(NamespacesConfig config) {
        this.enabled = config.enabled;
        this.namespaceConfigs.putAll(config.namespaceConfigs);
        this.classFilterConfig = config.classFilterConfig;
    }

    public NamespacesConfig(boolean enabled, Map<String, NamespaceConfig> namespaceConfigs) {
        this.enabled = enabled;
        this.namespaceConfigs.putAll(namespaceConfigs);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public NamespacesConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Adds the specified {@code namespaceConfig}, replacing any existing {@link NamespaceConfig} with the same
     * {@link NamespaceConfig#getName() name}.
     */
    public NamespacesConfig addNamespaceConfig(NamespaceConfig namespaceConfig) {
        namespaceConfigs.put(namespaceConfig.getName(), namespaceConfig);
        return this;
    }

    public NamespacesConfig removeNamespaceConfig(String namespace) {
        namespaceConfigs.remove(namespace);
        return this;
    }

    Map<String, NamespaceConfig> getNamespaceConfigs() {
        return Collections.unmodifiableMap(namespaceConfigs);
    }

    @Nullable
    public JavaSerializationFilterConfig getClassFilterConfig() {
        return classFilterConfig;
    }

    public void setClassFilterConfig(@Nullable JavaSerializationFilterConfig javaSerializationFilterConfig) {
        this.classFilterConfig = javaSerializationFilterConfig;
    }

    @Override
    public String toString() {
        return "NamespacesConfig{"
                + "enabled=" + enabled
                + ", namespaceConfigs=" + namespaceConfigs + '}'
                + ", classFilterConfig=" + classFilterConfig
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NamespacesConfig that = (NamespacesConfig) o;
        return enabled == that.enabled && Objects.equals(namespaceConfigs, that.namespaceConfigs)
                && Objects.equals(classFilterConfig, that.classFilterConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, namespaceConfigs, classFilterConfig);
    }
}
