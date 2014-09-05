/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

    public boolean isEnabled() {
        return enabled;
    }

    public ManagementCenterConfig setEnabled(final boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public ManagementCenterConfig setUrl(final String url) {
        this.url = url;
        return this;
    }

    public int getUpdateInterval() {
        return updateInterval;
    }

    public ManagementCenterConfig setUpdateInterval(final int updateInterval) {
        this.updateInterval = updateInterval;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ManagementCenterConfig");
        sb.append("{enabled=").append(enabled);
        sb.append(", url='").append(url).append('\'');
        sb.append(", updateInterval=").append(updateInterval);
        sb.append('}');
        return sb.toString();
    }
}
