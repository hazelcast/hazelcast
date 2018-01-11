/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Contains the configuration for a Management Center.
 */
public class ManagementCenterConfig {

    static final int UPDATE_INTERVAL = 3;

    private boolean enabled;

    private String url;

    private int updateInterval = UPDATE_INTERVAL;

    private MCMutualAuthConfig mutualAuthConfig;

    public ManagementCenterConfig() {
    }

    public ManagementCenterConfig(final String url, final int dataUpdateInterval) {
        this.url = url;
        this.updateInterval = dataUpdateInterval;
    }

    /**
     * {@code true} if management center is enabled, {@code false} if disabled.
     *
     * @return {@code true} if management center is enabled, {@code false} if disabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Set to {@code true} to enable management center, {@code false} to disable.
     *
     * @param enabled {@code true} to enable management center, {@code false} to disable
     * @return this management center config instance
     */
    public ManagementCenterConfig setEnabled(final boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Gets the URL where management center will work.
     *
     * @return the URL where management center will work
     */
    public String getUrl() {
        return url;
    }

    /**
     * Sets the URL where management center will work.
     *
     * @param url the URL where management center will work
     * @return this management center config instance
     */
    public ManagementCenterConfig setUrl(final String url) {
        this.url = url;
        return this;
    }

    /**
     * Gets the time frequency (in seconds) for which Management Center will take
     * information from the Hazelcast cluster.
     *
     * @return the time frequency (in seconds) for which Management Center will take
     * information from the Hazelcast cluster
     */
    public int getUpdateInterval() {
        return updateInterval;
    }

    /**
     * Sets the time frequency (in seconds) for which Management Center will take
     * information from the Hazelcast cluster.
     *
     * @param updateInterval the time frequency (in seconds) for which Management Center will take
     *                       information from the Hazelcast cluster
     * @return this management center config instance
     */
    public ManagementCenterConfig setUpdateInterval(final int updateInterval) {
        this.updateInterval = updateInterval;
        return this;
    }


    /**
     * Sets the management center mutual auth config
     *
     * @param mutualAuthConfig  the mutual auth config
     * @return the updated ManagementCenterConfig
     * @throws NullPointerException if mutualAuthConfig {@code null}
     */
    public ManagementCenterConfig setMutualAuthConfig(MCMutualAuthConfig mutualAuthConfig) {
        checkNotNull(mutualAuthConfig);
        this.mutualAuthConfig = mutualAuthConfig;
        return this;
    }

    /**
     * Gets a property.
     *
     * @return the value of the property, null if not found
     * @throws NullPointerException if name is {@code null}
     */
    public MCMutualAuthConfig getMutualAuthConfig() {
        return mutualAuthConfig;
    }

    @Override
    public String toString() {
        return "ManagementCenterConfig{"
                + "enabled=" + enabled
                + ", url='" + url + "'"
                + ", updateInterval=" + updateInterval
                + ", mcMutualAuthConfig=" + mutualAuthConfig
                + "}";
    }
}
