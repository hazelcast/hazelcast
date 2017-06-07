/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
 * Contains the configuration for a Management Center.
 */
public class ManagementCenterConfig {

    static final int UPDATE_INTERVAL = 3;

    private boolean enabled;

    private String url;

    private int updateInterval = UPDATE_INTERVAL;

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

    @Override
    public String toString() {
        return "ManagementCenterConfig{enabled=" + enabled + ", url='" + url + "', updateInterval=" + updateInterval + '}';
    }
}
