/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Configures the Persistence stores.
 * <p>
 * Persistence stores are used to hold copy of in-memory data in
 * disk to be able to restart very fast without needing to load
 * data from a central storage.
 * <p>
 * PersistenceConfig configures whether persistence is enabled,
 * where disk data will be stored, should data be persisted
 * sync or async etc.
 *
 * <br><br>
 * Note: If either, but not both, persistence ({@code PersistenceConfig}) or
 * hot-restart-persistence ({@code HotRestartPersistenceConfig}) is enabled,
 * Hazelcast will use the configuration of the enabled element. If both are
 * enabled, Hazelcast will use the persistence ({@code PersistenceConfig})
 * configuration. hot-restart-persistence element (and thus {@code HotRestartPersistenceConfig})
 * will be removed in a future release.
 */
public class PersistenceConfig {
    /** Default directory name for the Persistence store's home */
    public static final String PERSISTENCE_BASE_DIR_DEFAULT = "persistence";

    /**
     * Default validation timeout
     */
    public static final int DEFAULT_VALIDATION_TIMEOUT = 2 * 60;

    /**
     * Default load timeout
     */
    public static final int DEFAULT_DATA_LOAD_TIMEOUT = 15 * 60;

    /**
     * Default level of parallelism in Persistence. Controls the number
     * of instances operating with a single IO thread and a
     * single GC thread.
     */
    public static final int DEFAULT_PARALLELISM = 1;
    /**
     *
     */
    public static final int DEFAULT_REBALANCE_DELAY = 0;

    private boolean enabled;
    private File baseDir = new File(PERSISTENCE_BASE_DIR_DEFAULT);
    private File backupDir;
    private int parallelism = DEFAULT_PARALLELISM;
    private int validationTimeoutSeconds = DEFAULT_VALIDATION_TIMEOUT;
    private int dataLoadTimeoutSeconds = DEFAULT_DATA_LOAD_TIMEOUT;
    private PersistenceClusterDataRecoveryPolicy clusterDataRecoveryPolicy
            = PersistenceClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY;
    private boolean autoRemoveStaleData = true;
    private EncryptionAtRestConfig encryptionAtRestConfig = new EncryptionAtRestConfig();
    private int rebalanceDelaySeconds = DEFAULT_REBALANCE_DELAY;

    /**
     * Returns whether persistence enabled on this member.
     *
     * @return true if persistence enabled, false otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets whether persistence is enabled on this member.
     *
     * @return PersistenceConfig
     */
    public PersistenceConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Returns the policy to be used when the cluster is started
     *
     * @return the policy to be used when the cluster is started
     */
    public PersistenceClusterDataRecoveryPolicy getClusterDataRecoveryPolicy() {
        return clusterDataRecoveryPolicy;
    }

    /**
     * Sets the policy to be used when the cluster is started
     *
     * @param clusterDataRecoveryPolicy the policy to be used when the cluster is started
     *
     * @return PersistenceConfig
     */
    public PersistenceConfig setClusterDataRecoveryPolicy(PersistenceClusterDataRecoveryPolicy
                                                                            clusterDataRecoveryPolicy) {
        this.clusterDataRecoveryPolicy = clusterDataRecoveryPolicy;
        return this;
    }

    /**
     * Base directory for all Persistence stores. Can be an absolute or relative path to the node startup directory.
     */
    public File getBaseDir() {
        return baseDir;
    }

    /**
     * Sets base directory for all Persistence stores. Can be an absolute or relative path to the node startup directory.
     *
     * @param baseDir home directory
     * @return PersistenceConfig
     */
    public PersistenceConfig setBaseDir(File baseDir) {
        checkNotNull(baseDir, "Base directory cannot be null!");
        this.baseDir = baseDir;
        return this;
    }

    /**
     * Base directory for persistence backups. Each new backup will be created in a separate directory inside this one.
     * Can be an absolute or relative path to the node startup directory.
     */
    public File getBackupDir() {
        return backupDir;
    }

    /**
     * Sets base directory for all Hot Restart stores.
     *
     * @param backupDir home directory
     * @return PersistenceConfig
     */
    public PersistenceConfig setBackupDir(File backupDir) {
        this.backupDir = backupDir;
        return this;
    }

    /**
     * Gets the configured number of Persistence store instance to create for one Hazelcast instance.
     */
    public int getParallelism() {
        return parallelism;
    }

    /**
     * Sets the number of Persistence store instances to create for one Hazelcast instance.
     */
    public PersistenceConfig setParallelism(int parallelism) {
        checkPositive(parallelism, "Palallelism must be a positive integer");
        this.parallelism = parallelism;
        return this;
    }

    /**
     * Returns configured validation timeout for restart process.
     *
     * @return validation timeout in seconds
     */
    public int getValidationTimeoutSeconds() {
        return validationTimeoutSeconds;
    }

    /**
     * Sets validation timeout for persistence restart process, includes validating
     * cluster members expected to join and partition table on all cluster.
     *
     * @param validationTimeoutSeconds validation timeout in seconds
     * @return PersistenceConfig
     */
    public PersistenceConfig setValidationTimeoutSeconds(int validationTimeoutSeconds) {
        checkPositive(validationTimeoutSeconds, "Validation timeout should be positive!");
        this.validationTimeoutSeconds = validationTimeoutSeconds;
        return this;
    }

    /**
     * Returns configured data load timeout for persistence restart process.
     *
     * @return data load timeout in seconds
     */
    public int getDataLoadTimeoutSeconds() {
        return dataLoadTimeoutSeconds;
    }

    /**
     * Sets data load timeout for persistence restart process,
     * all members in the cluster should complete restoring their local data
     * before this timeout.
     *
     * @param dataLoadTimeoutSeconds data load timeout in seconds
     * @return PersistenceConfig
     */
    public PersistenceConfig setDataLoadTimeoutSeconds(int dataLoadTimeoutSeconds) {
        checkPositive(dataLoadTimeoutSeconds, "Load timeout should be positive!");
        this.dataLoadTimeoutSeconds = dataLoadTimeoutSeconds;
        return this;
    }

    /**
     * Returns whether or not automatic removal of stale Persistence restart data is enabled.
     *
     * @return whether or not automatically removal of stale data is enabled
     */
    public boolean isAutoRemoveStaleData() {
        return autoRemoveStaleData;
    }

    /**
     * Sets whether or not automatic removal of stale Persistence restart data is enabled.
     * <p>
     * When a member terminates or crashes when cluster state is {@link com.hazelcast.cluster.ClusterState#ACTIVE},
     * remaining members redistributes data among themselves and data persisted on terminated member's storage becomes
     * stale. That terminated member cannot rejoin the cluster without removing Persistence data.
     * When auto-removal of stale Persistence data is enabled, while restarting that member, Persistence data is
     * automatically removed and it joins the cluster as a completely new member.
     * Otherwise, Persistence data should be removed manually.
     *
     * @param autoRemoveStaleData {@code true} to enable auto-removal of stale data, {@code false} otherwise
     * @return PersistenceConfig
     */
    public PersistenceConfig setAutoRemoveStaleData(boolean autoRemoveStaleData) {
        this.autoRemoveStaleData = autoRemoveStaleData;
        return this;
    }

    /**
     * Sets the Persistence Encryption at Rest configuration.
     * @param encryptionAtRestConfig the Encryption at Rest configuration
     * @return PersistenceConfig
     */
    public PersistenceConfig setEncryptionAtRestConfig(EncryptionAtRestConfig encryptionAtRestConfig) {
        checkNotNull(encryptionAtRestConfig, "Encryption at rest config cannot be null!");
        this.encryptionAtRestConfig = encryptionAtRestConfig;
        return this;
    }

    /**
     * Returns the Persistence Encryption at Rest configuration.
     * @return the Encryption at Rest configuration
     */
    public EncryptionAtRestConfig getEncryptionAtRestConfig() {
        return encryptionAtRestConfig;
    }

    /**
     * Get the configured rebalance delay in seconds.
     *
     * @return  the configured rebalance delay in seconds.
     * @see     #setRebalanceDelaySeconds(int)
     * @since   5.0
     */
    public int getRebalanceDelaySeconds() {
        return rebalanceDelaySeconds;
    }

    /**
     * Set the rebalance delay in seconds. This is the time
     * to wait before triggering automatic partition rebalancing
     * after a member leaves the cluster unexpectedly. Unexpectedly in this context
     * means that a member leaves the cluster by means other than graceful shutdown:
     * programmatic termination (eg {@code LifecycleService.terminate()}), a
     * process crash or network partition.
     * Default is 0, which means rebalancing is triggered immediately.
     * Setting this to a higher value will allow some time for members that are gone
     * to rejoin the cluster. The benefit is that partition rebalancing in this
     * case will be avoided, saving the burden of migrating partition data over
     * the network.
     * Do not use this option if your cluster also stores in-memory data. This option
     * stops the cluster from migrating in-memory data. As a result any data that is
     * not persisted will be lost if the member restarts within the configured delay,
     * including backups.
     * While members are gone, operations on partitions for which the owner is missing
     * may fail immediately or will be retried until the member rejoins or operation
     * timeout is exceeded.
     * Notice that this delay only applies when cluster members leave the cluster;
     * when the cluster is being scaled up and members are being added, partition
     * rebalancing will be triggered immediately (subject to limitations imposed
     * by the current cluster state).
     *
     * @param rebalanceDelaySeconds the desired non-negative rebalance delay in seconds.
     * @return  PersistenceConfig
     * @since   5.0
     */
    public PersistenceConfig setRebalanceDelaySeconds(int rebalanceDelaySeconds) {
        checkNotNegative(rebalanceDelaySeconds, "Rebalance delay cannot be negative.");
        this.rebalanceDelaySeconds = rebalanceDelaySeconds;
        return this;
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PersistenceConfig)) {
            return false;
        }

        PersistenceConfig that = (PersistenceConfig) o;
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
        if (rebalanceDelaySeconds != that.rebalanceDelaySeconds) {
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
        result = 31 * result + rebalanceDelaySeconds;
        result = 31 * result + (clusterDataRecoveryPolicy != null ? clusterDataRecoveryPolicy.hashCode() : 0);
        result = 31 * result + (autoRemoveStaleData ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PersistenceConfig{"
                + "enabled=" + enabled
                + ", baseDir=" + baseDir
                + ", backupDir=" + backupDir
                + ", parallelism=" + parallelism
                + ", validationTimeoutSeconds=" + validationTimeoutSeconds
                + ", dataLoadTimeoutSeconds=" + dataLoadTimeoutSeconds
                + ", rebalanceDelaySeconds=" + dataLoadTimeoutSeconds
                + ", clusterDataRecoveryPolicy=" + clusterDataRecoveryPolicy
                + ", autoRemoveStaleData=" + autoRemoveStaleData
                + ", encryptionAtRestConfig=" + encryptionAtRestConfig
                + '}';
    }
}
