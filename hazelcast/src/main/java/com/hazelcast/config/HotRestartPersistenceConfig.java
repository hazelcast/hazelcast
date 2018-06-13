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

import java.io.File;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Configures the Hot Restart stores.
 * <p>
 * Hot restart stores are used to hold copy of in-memory data in
 * disk to be able to restart very fast without needing to load
 * data from a central storage.
 * <p>
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

    /**
     * Default level of parallelism in Hot Restart Persistence. Controls the number
     * of Hot Restart Store instances, each operating with a single IO thread and a
     * single GC thread.
     */
    public static final int DEFAULT_PARALLELISM = 1;

    private boolean enabled;
    private File baseDir = new File(HOT_RESTART_BASE_DIR_DEFAULT);
    private File backupDir;
    private int parallelism = DEFAULT_PARALLELISM;
    private int validationTimeoutSeconds = DEFAULT_VALIDATION_TIMEOUT;
    private int dataLoadTimeoutSeconds = DEFAULT_DATA_LOAD_TIMEOUT;
    private HotRestartClusterDataRecoveryPolicy clusterDataRecoveryPolicy
            = HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY;

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
     * Returns the policy to be used when the cluster is started
     *
     * @return the policy to be used when the cluster is started
     */
    public HotRestartClusterDataRecoveryPolicy getClusterDataRecoveryPolicy() {
        return clusterDataRecoveryPolicy;
    }

    /**
     * Sets the policy to be used when the cluster is started
     *
     * @param clusterDataRecoveryPolicy the policy to be used when the cluster is started
     *
     * @return HotRestartConfig
     */
    public HotRestartPersistenceConfig setClusterDataRecoveryPolicy(HotRestartClusterDataRecoveryPolicy
                                                                            clusterDataRecoveryPolicy) {
        this.clusterDataRecoveryPolicy = clusterDataRecoveryPolicy;
        return this;
    }

    /**
     * Base directory for all Hot Restart stores. Can be an absolute or relative path to the node startup directory.
     */
    public File getBaseDir() {
        return baseDir;
    }

    /**
     * Sets base directory for all Hot Restart stores. Can be an absolute or relative path to the node startup directory.
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
     * Base directory for hot backups. Each new backup will be created in a separate directory inside this one.
     * Can be an absolute or relative path to the node startup directory.
     */
    public File getBackupDir() {
        return backupDir;
    }

    /**
     * Sets base directory for all Hot Restart stores.
     *
     * @param backupDir home directory
     * @return HotRestartConfig
     */
    public HotRestartPersistenceConfig setBackupDir(File backupDir) {
        this.backupDir = backupDir;
        return this;
    }

    /**
     * Gets the configured number of Hot Restart store instance to create for one Hazelcast instance.
     */
    public int getParallelism() {
        return parallelism;
    }

    /**
     * Sets the number of Hot Restart store instances to create for one Hazelcast instance.
     */
    public HotRestartPersistenceConfig setParallelism(int parallelism) {
        checkPositive(parallelism, "Palallelism must be a positive integer");
        this.parallelism = parallelism;
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

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HotRestartPersistenceConfig)) {
            return false;
        }

        HotRestartPersistenceConfig that = (HotRestartPersistenceConfig) o;
        if (enabled != that.enabled) {
            return false;
        }
        if (parallelism != that.parallelism) {
            return false;
        }
        if (validationTimeoutSeconds != that.validationTimeoutSeconds) {
            return false;
        }
        if (dataLoadTimeoutSeconds != that.dataLoadTimeoutSeconds) {
            return false;
        }
        if (baseDir != null ? !baseDir.equals(that.baseDir) : that.baseDir != null) {
            return false;
        }
        if (backupDir != null ? !backupDir.equals(that.backupDir) : that.backupDir != null) {
            return false;
        }
        return clusterDataRecoveryPolicy == that.clusterDataRecoveryPolicy;
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (baseDir != null ? baseDir.hashCode() : 0);
        result = 31 * result + (backupDir != null ? backupDir.hashCode() : 0);
        result = 31 * result + parallelism;
        result = 31 * result + validationTimeoutSeconds;
        result = 31 * result + dataLoadTimeoutSeconds;
        result = 31 * result + (clusterDataRecoveryPolicy != null ? clusterDataRecoveryPolicy.hashCode() : 0);
        return result;
    }
}
