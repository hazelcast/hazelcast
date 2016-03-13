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

import java.io.File;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;

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
public class HotRestartPersistenceConfig {
    /** Default directory name for the Hot Restart store's home */
    public static final String HOT_RESTART_BASE_DIR_DEFAULT = "hot-restart";

    /**
     * Default validation timeout
     */
    public static final int DEFAULT_VALIDATION_TIMEOUT = 2 * 60;

    /**
     * Default load timeout
     */
    public static final int DEFAULT_DATA_LOAD_TIMEOUT = 15 * 60;

    private boolean enabled;
    private File baseDir = new File(HOT_RESTART_BASE_DIR_DEFAULT);
    private int validationTimeoutSeconds = DEFAULT_VALIDATION_TIMEOUT;
    private int dataLoadTimeoutSeconds = DEFAULT_DATA_LOAD_TIMEOUT;

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
    public HotRestartPersistenceConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Base directory for all Hot Restart stores.
     */
    public File getBaseDir() {
        return baseDir;
    }

    /**
     * Sets base directory for all Hot Restart stores.
     *
     * @param baseDir home directory
     * @return HotRestartConfig
     */
    public HotRestartPersistenceConfig setBaseDir(File baseDir) {
        checkNotNull(baseDir, "Base directory cannot be null!");
        this.baseDir = baseDir;
        return this;
    }

    /**
     * Returns configured validation timeout for hot-restart process.
     *
     * @return validation timeout in seconds
     */
    public int getValidationTimeoutSeconds() {
        return validationTimeoutSeconds;
    }

    /**
     * Sets validation timeout for hot-restart process, includes validating
     * cluster members expected to join and partition table on all cluster.
     *
     * @param validationTimeoutSeconds validation timeout in seconds
     * @return HotRestartConfig
     */
    public HotRestartPersistenceConfig setValidationTimeoutSeconds(int validationTimeoutSeconds) {
        checkPositive(validationTimeoutSeconds, "Validation timeout should be positive!");
        this.validationTimeoutSeconds = validationTimeoutSeconds;
        return this;
    }

    /**
     * Returns configured data load timeout for hot-restart process.
     *
     * @return data load timeout in seconds
     */
    public int getDataLoadTimeoutSeconds() {
        return dataLoadTimeoutSeconds;
    }

    /**
     * Sets data load timeout for hot-restart process,
     * all members in the cluster should complete restoring their local data
     * before this timeout.
     *
     * @param dataLoadTimeoutSeconds data load timeout in seconds
     * @return HotRestartConfig
     */
    public HotRestartPersistenceConfig setDataLoadTimeoutSeconds(int dataLoadTimeoutSeconds) {
        checkPositive(dataLoadTimeoutSeconds, "Load timeout should be positive!");
        this.dataLoadTimeoutSeconds = dataLoadTimeoutSeconds;
        return this;
    }
}
