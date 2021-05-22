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

package com.hazelcast.jet.config;

import com.hazelcast.internal.util.Preconditions;

import javax.annotation.Nonnull;

/**
 * Configuration object for a Jet instance.
 *
 * @since 3.0
 */
public class JetConfig {

    private InstanceConfig instanceConfig = new InstanceConfig();
    private EdgeConfig defaultEdgeConfig = new EdgeConfig();
    private boolean enabled = true;
    private boolean resourceUploadEnabled;

    /**
     * Creates a new, empty {@code JetConfig} with the default configuration.
     * Doesn't consider any configuration XML files.
     */
    public JetConfig() {
    }

    /**
     * Returns if Jet is enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets if Jet is enabled
     */
    public JetConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Returns if uploading resources when submitting the job enabled
     */
    public boolean isResourceUploadEnabled() {
        return resourceUploadEnabled;
    }

    /**
     * Sets if uploading resources when submitting the job enabled
     */
    public JetConfig setResourceUploadEnabled(boolean resourceUploadEnabled) {
        this.resourceUploadEnabled = resourceUploadEnabled;
        return this;
    }

    /**
     * Returns the Jet instance config.
     */
    @Nonnull
    public InstanceConfig getInstanceConfig() {
        return instanceConfig;
    }

    /**
     * Sets the Jet instance config.
     */
    @Nonnull
    public JetConfig setInstanceConfig(@Nonnull InstanceConfig instanceConfig) {
        Preconditions.checkNotNull(instanceConfig, "instanceConfig");
        this.instanceConfig = instanceConfig;
        return this;
    }

    /**
     * Returns the default DAG edge configuration.
     */
    @Nonnull
    public EdgeConfig getDefaultEdgeConfig() {
        return defaultEdgeConfig;
    }

    /**
     * Sets the configuration object that specifies the defaults to use
     * for a DAG edge configuration.
     */
    @Nonnull
    public JetConfig setDefaultEdgeConfig(@Nonnull EdgeConfig defaultEdgeConfig) {
        Preconditions.checkNotNull(defaultEdgeConfig, "defaultEdgeConfig");
        this.defaultEdgeConfig = defaultEdgeConfig;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JetConfig jetConfig = (JetConfig) o;

        if (enabled != jetConfig.enabled) {
            return false;
        }
        if (resourceUploadEnabled != jetConfig.resourceUploadEnabled) {
            return false;
        }
        if (!instanceConfig.equals(jetConfig.instanceConfig)) {
            return false;
        }
        return defaultEdgeConfig.equals(jetConfig.defaultEdgeConfig);
    }

    @Override
    public int hashCode() {
        int result = instanceConfig.hashCode();
        result = 31 * result + defaultEdgeConfig.hashCode();
        result = 31 * result + (enabled ? 1 : 0);
        result = 31 * result + (resourceUploadEnabled ? 1 : 0);
        return result;
    }
}
