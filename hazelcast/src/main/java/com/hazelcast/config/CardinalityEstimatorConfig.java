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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.merge.HyperLogLogMergePolicy;

import java.io.IOException;
import java.util.Arrays;

import static com.hazelcast.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.util.Preconditions.checkBackupCount;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Configuration options for the {@link com.hazelcast.cardinality.CardinalityEstimator}
 */
public class CardinalityEstimatorConfig implements IdentifiedDataSerializable, Versioned {

    /**
     * The number of sync backups per estimator
     */
    public static final int DEFAULT_SYNC_BACKUP_COUNT = 1;

    /**
     * The number of async backups per estimator
     */
    public static final int DEFAULT_ASYNC_BACKUP_COUNT = 0;

    /**
     * The default merge policy used for cardinality estimators
     */
    public static final MergePolicyConfig DEFAULT_MERGE_POLICY_CONFIG
            = new MergePolicyConfig(HyperLogLogMergePolicy.class.getSimpleName(), MergePolicyConfig.DEFAULT_BATCH_SIZE);

    private static final String[] ALLOWED_POLICIES = {
            "com.hazelcast.spi.merge.DiscardMergePolicy", "DiscardMergePolicy",
            "com.hazelcast.spi.merge.HyperLogLogMergePolicy", "HyperLogLogMergePolicy",
            "com.hazelcast.spi.merge.PassThroughMergePolicy", "PassThroughMergePolicy",
            "com.hazelcast.spi.merge.PutIfAbsentMergePolicy", "PutIfAbsentMergePolicy",
    };

    private String name = "default";

    private int backupCount = DEFAULT_SYNC_BACKUP_COUNT;

    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;

    private String quorumName;

    private MergePolicyConfig mergePolicyConfig = DEFAULT_MERGE_POLICY_CONFIG;

    private transient CardinalityEstimatorConfigReadOnly readOnly;

    public CardinalityEstimatorConfig() {
    }

    public CardinalityEstimatorConfig(String name) {
        this.name = name;
    }

    public CardinalityEstimatorConfig(String name, int backupCount, int asyncBackupCount) {
        this(name, backupCount, asyncBackupCount, DEFAULT_MERGE_POLICY_CONFIG);
    }

    public CardinalityEstimatorConfig(String name, int backupCount, int asyncBackupCount, MergePolicyConfig mergePolicyConfig) {
        this(name, backupCount, asyncBackupCount, "", mergePolicyConfig);
    }

    public CardinalityEstimatorConfig(String name, int backupCount, int asyncBackupCount,
                                      String quorumName, MergePolicyConfig mergePolicyConfig) {
        this.name = name;
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
        this.quorumName = quorumName;
        this.mergePolicyConfig = mergePolicyConfig;
        validate();
    }

    public CardinalityEstimatorConfig(CardinalityEstimatorConfig config) {
        this(config.getName(), config.getBackupCount(), config.getAsyncBackupCount(),
                config.getQuorumName(), config.getMergePolicyConfig());
    }

    /**
     * Gets the name of the cardinality estimator.
     *
     * @return the name of the estimator
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the cardinality estimator.
     *
     * @param name the name of the estimator
     * @return the cardinality estimator config instance
     */
    public CardinalityEstimatorConfig setName(String name) {
        checkNotNull(name);
        this.name = name;
        return this;
    }

    /**
     * Gets the {@link MergePolicyConfig} for the cardinality estimator.
     *
     * @return the {@link MergePolicyConfig} for the cardinality estimator
     */
    public MergePolicyConfig getMergePolicyConfig() {
        return mergePolicyConfig;
    }

    /**
     * Sets the {@link MergePolicyConfig} for the scheduler.
     *
     * @return this executor config instance
     */
    public CardinalityEstimatorConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        this.mergePolicyConfig = checkNotNull(mergePolicyConfig, "mergePolicyConfig cannot be null");
        validate();
        return this;
    }


    /**
     * Gets the number of synchronous backups.
     *
     * @return number of synchronous backups
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Sets the number of synchronous backups.
     *
     * @param backupCount the number of synchronous backups to set
     * @return the updated CardinalityEstimatorConfig
     * @throws IllegalArgumentException if backupCount smaller than 0,
     *                                  or larger than the maximum number of backup
     *                                  or the sum of the backups and async backups is larger than the maximum number of backups
     * @see #setAsyncBackupCount(int)
     * @see #getBackupCount()
     */
    public CardinalityEstimatorConfig setBackupCount(int backupCount) {
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Gets the number of synchronous backups.
     *
     * @return number of synchronous backups
     */
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Sets the number of synchronous backups.
     *
     * @param asyncBackupCount the number of synchronous backups to set
     * @return the updated CardinalityEstimatorConfig
     * @throws IllegalArgumentException if backupCount smaller than 0,
     *                                  or larger than the maximum number of backup
     *                                  or the sum of the backups and async backups is larger than the maximum number of backups
     * @see #setAsyncBackupCount(int)
     * @see #getBackupCount()
     */
    public CardinalityEstimatorConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Returns the total number of backups: backupCount plus asyncBackupCount.
     *
     * @return the total number of backups: backupCount plus asyncBackupCount
     */
    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    /**
     * Returns the quorum name for operations.
     *
     * @return the quorum name
     */
    public String getQuorumName() {
        return quorumName;
    }

    /**
     * Sets the quorum name for operations.
     *
     * @param quorumName the quorum name
     * @return the updated configuration
     */
    public CardinalityEstimatorConfig setQuorumName(String quorumName) {
        this.quorumName = quorumName;
        return this;
    }


    @Override
    public String toString() {
        return "CardinalityEstimatorConfig{" + "name='" + name + '\'' + ", backupCount=" + backupCount + ", asyncBackupCount="
                + asyncBackupCount + ", readOnly=" + readOnly + ", quorumName=" + quorumName + ", mergePolicyConfig="
                + mergePolicyConfig + '}';
    }

    CardinalityEstimatorConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new CardinalityEstimatorConfigReadOnly(this);
        }
        return readOnly;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.CARDINALITY_ESTIMATOR_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);
        // RU_COMPAT_3_9
        if (out.getVersion().isGreaterOrEqual(Versions.V3_10)) {
            out.writeUTF(quorumName);
            out.writeObject(mergePolicyConfig);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();
        // RU_COMPAT_3_9
        if (in.getVersion().isGreaterOrEqual(Versions.V3_10)) {
            quorumName = in.readUTF();
            mergePolicyConfig = in.readObject();
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CardinalityEstimatorConfig)) {
            return false;
        }

        CardinalityEstimatorConfig that = (CardinalityEstimatorConfig) o;
        if (backupCount != that.backupCount) {
            return false;
        }
        if (asyncBackupCount != that.asyncBackupCount) {
            return false;
        }
        if (quorumName != null ? !quorumName.equals(that.quorumName) : that.quorumName != null) {
            return false;
        }
        if (mergePolicyConfig != null ? !mergePolicyConfig.equals(that.mergePolicyConfig) : that.mergePolicyConfig != null) {
            return false;
        }

        return name.equals(that.name);
    }

    @Override
    public final int hashCode() {
        int result = name.hashCode();
        result = 31 * result + backupCount;
        result = 31 * result + asyncBackupCount;
        result = 31 * result + (quorumName != null ? quorumName.hashCode() : 0);
        result = 31 * result + (mergePolicyConfig != null ? mergePolicyConfig.hashCode() : 0);
        return result;
    }

    public final void validate() {
        if (!Arrays.asList(ALLOWED_POLICIES).contains(mergePolicyConfig.getPolicy())) {
            throw new InvalidConfigurationException(format("Policy %s is not allowed as a merge-policy "
                    + "for CardinalityEstimator.", mergePolicyConfig.getPolicy()));
        }
    }

    // not private for testing
    static class CardinalityEstimatorConfigReadOnly extends CardinalityEstimatorConfig {

        CardinalityEstimatorConfigReadOnly(CardinalityEstimatorConfig config) {
            super(config);
        }

        @Override
        public CardinalityEstimatorConfig setName(String name) {
            throw new UnsupportedOperationException("This config is read-only cardinality estimator: " + getName());
        }

        @Override
        public CardinalityEstimatorConfig setBackupCount(int backupCount) {
            throw new UnsupportedOperationException("This config is read-only cardinality estimator: " + getName());
        }

        @Override
        public CardinalityEstimatorConfig setAsyncBackupCount(int asyncBackupCount) {
            throw new UnsupportedOperationException("This config is read-only cardinality estimator: " + getName());
        }

        @Override
        public CardinalityEstimatorConfig setQuorumName(String quorumName) {
            throw new UnsupportedOperationException("This config is read-only cardinality estimator: " + getName());
        }

        @Override
        public CardinalityEstimatorConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
            throw new UnsupportedOperationException("This config is read-only cardinality estimator: " + getName());
        }
    }
}
