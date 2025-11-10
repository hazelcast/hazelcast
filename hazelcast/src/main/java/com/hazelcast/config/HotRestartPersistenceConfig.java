/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

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
 *
 *<br><br>
 * Note: If either, but not both, persistence ({@code PersistenceConfig}) or
 * hot-restart-persistence ({@code HotRestartPersistenceConfig}) is enabled,
 * Hazelcast will use the configuration of the enabled element. If both are
 * enabled, Hazelcast will use the persistence ({@code PersistenceConfig})
 * configuration. hot-restart-persistence element (and thus {@code HotRestartPersistenceConfig})
 * will be removed in a future release.
 *
 * @deprecated use {@link PersistenceConfig}
 */
@Deprecated(since = "5.0")
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
    private boolean autoRemoveStaleData = true;
    private EncryptionAtRestConfig encryptionAtRestConfig = new EncryptionAtRestConfig();

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
     * @return HotRestartPersistenceConfig
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
     * @return HotRestartPersistenceConfig
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
     * @return HotRestartPersistenceConfig
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
     * @return HotRestartPersistenceConfig
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
        checkPositive(parallelism, "Parallelism must be a positive integer");
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
     * @return HotRestartPersistenceConfig
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
     * @return HotRestartPersistenceConfig
     */
    public HotRestartPersistenceConfig setDataLoadTimeoutSeconds(int dataLoadTimeoutSeconds) {
        checkPositive(dataLoadTimeoutSeconds, "Load timeout should be positive!");
        this.dataLoadTimeoutSeconds = dataLoadTimeoutSeconds;
        return this;
    }

    /**
     * Returns whether automatic removal of stale Hot Restart data is enabled.
     *
     * @return whether automatic removal of stale data is enabled
     */
    public boolean isAutoRemoveStaleData() {
        return autoRemoveStaleData;
    }

    /**
     * Sets whether automatic removal of stale Hot Restart data is enabled.
     * <p>
     * When a member terminates or crashes when cluster state is {@link com.hazelcast.cluster.ClusterState#ACTIVE},
     * remaining members redistributes data among themselves and data persisted on terminated member's storage becomes
     * stale. That terminated member cannot rejoin the cluster without removing Hot Restart data.
     * When auto-removal of stale Hot Restart data is enabled, while restarting that member, Hot Restart data is
     * automatically removed, and it joins the cluster as a completely new member.
     * Otherwise, Hot Restart data should be removed manually.
     *
     * @param autoRemoveStaleData {@code true} to enable auto-removal of stale data, {@code false} otherwise
     * @return HotRestartPersistenceConfig
     */
    public HotRestartPersistenceConfig setAutoRemoveStaleData(boolean autoRemoveStaleData) {
        this.autoRemoveStaleData = autoRemoveStaleData;
        return this;
    }

    /**
     * Sets the Hot Restart Encryption at Rest configuration.
     * @param encryptionAtRestConfig the Encryption at Rest configuration
     * @return HotRestartPersistenceConfig§
     */
    public HotRestartPersistenceConfig setEncryptionAtRestConfig(EncryptionAtRestConfig encryptionAtRestConfig) {
        checkNotNull(encryptionAtRestConfig, "Encryption at rest config cannot be null!");
        this.encryptionAtRestConfig = encryptionAtRestConfig;
        return this;
    }

    /**
     * Returns the Hot Restart Encryption at Rest configuration.
     * @return the Encryption at Rest configuration
     */
    public EncryptionAtRestConfig getEncryptionAtRestConfig() {
        return encryptionAtRestConfig;
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HotRestartPersistenceConfig that)) {
            return false;
        }

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
        if (autoRemoveStaleData != that.autoRemoveStaleData) {
            return false;
        }
        if (!Objects.equals(baseDir, that.baseDir)) {
            return false;
        }
        if (!Objects.equals(backupDir, that.backupDir)) {
            return false;
        }
        if (!Objects.equals(encryptionAtRestConfig, that.encryptionAtRestConfig)) {
            return false;
        }
        return clusterDataRecoveryPolicy == that.clusterDataRecoveryPolicy;
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (baseDir != null ? baseDir.hashCode() : 0);
        result = 31 * result + (backupDir != null ? backupDir.hashCode() : 0);
        result = 31 * result + (encryptionAtRestConfig != null ? encryptionAtRestConfig.hashCode() : 0);
        result = 31 * result + parallelism;
        result = 31 * result + validationTimeoutSeconds;
        result = 31 * result + dataLoadTimeoutSeconds;
        result = 31 * result + (clusterDataRecoveryPolicy != null ? clusterDataRecoveryPolicy.hashCode() : 0);
        result = 31 * result + (autoRemoveStaleData ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "HotRestartPersistenceConfig{"
                + "enabled=" + enabled
                + ", baseDir=" + baseDir
                + ", backupDir=" + backupDir
                + ", parallelism=" + parallelism
                + ", validationTimeoutSeconds=" + validationTimeoutSeconds
                + ", dataLoadTimeoutSeconds=" + dataLoadTimeoutSeconds
                + ", clusterDataRecoveryPolicy=" + clusterDataRecoveryPolicy
                + ", autoRemoveStaleData=" + autoRemoveStaleData
                + ", encryptionAtRestConfig=" + encryptionAtRestConfig
                + '}';
    }
}
