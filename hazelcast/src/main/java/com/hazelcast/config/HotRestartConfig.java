/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
 * Configures the Hot Restart Persistence per Hazelcast data structure.
 */
public class HotRestartConfig {

    private boolean enabled;
    private boolean fsync;

    public HotRestartConfig() {
    }

    public HotRestartConfig(HotRestartConfig hotRestartConfig) {
        enabled = hotRestartConfig.enabled;
        fsync = hotRestartConfig.fsync;
    }

    /**
     * Returns whether hot restart enabled on related data structure.
     *
     * @return true if hot restart enabled, false otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets whether hot restart is enabled on related data structure.
     *
     * @return HotRestartConfig
     */
    public HotRestartConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Returns whether disk write should be followed by an {@code fsync()} system call.
     *
     * @return true if fsync is be called after disk write, false otherwise
     */
    public boolean isFsync() {
        return fsync;
    }

    /**
     * Sets whether disk write should be followed by an {@code fsync()} system call.
     *
     * @param fsync fsync
     * @return this HotRestartConfig
     */
    public HotRestartConfig setFsync(boolean fsync) {
        this.fsync = fsync;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HotRestartConfig{");
        sb.append("enabled=").append(enabled);
        sb.append(", fsync=").append(fsync);
        sb.append('}');
        return sb.toString();
    }
}
