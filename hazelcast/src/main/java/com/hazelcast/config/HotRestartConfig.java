/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import java.io.File;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Configures the Hot Restart stores.
 * <p/>
 * Hot restart stores are used to hold copy of in-memory data in
 * disk to be able to restart very fast without needing to load
 * data from a central storage.
 * <p/>
 * HotRestartConfig configures whether hot restart is enabled,
 * where disk data will be stored, should data be persisted
 * sync or async etc.
 */
public class HotRestartConfig {

    /**
     * Default hot-restart home directory
     */
    public static final String HOT_RESTART_HOME_DEFAULT = "hot-restart";

    private boolean enabled;
    private File homeDir = new File(HOT_RESTART_HOME_DEFAULT);

    /**
     * Returns whether hot restart enabled on this member.
     *
     * @return true if hot restart enabled, false otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets whether hot restart is enabled on this member.
     *
     * @return HotRestartConfig
     */
    public HotRestartConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Home directory for all Hot Restart stores.
     */
    public File getHomeDir() {
        return homeDir;
    }

    /**
     * Sets home directory for all Hot Restart stores.
     *
     * @param homeDir home directory
     * @return HotRestartConfig
     */
    public HotRestartConfig setHomeDir(File homeDir) {
        checkNotNull(homeDir, "Home directory cannot be null!");
        this.homeDir = homeDir;
        return this;
    }
}
