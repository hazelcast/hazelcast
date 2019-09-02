/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
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
public class CardinalityEstimatorConfig implements IdentifiedDataSerializable, NamedConfig {

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

    private String splitBrainProtectionName;

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
                                      String splitBrainProtectionName, MergePolicyConfig mergePolicyConfig) {
        this.name = name;
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
        this.splitBrainProtectionName = splitBrainProtectionName;
        this.mergePolicyConfig = mergePolicyConfig;
        validate();
    }

    public CardinalityEstimatorConfig(CardinalityEstimatorConfig config) {
        this(config.getName(), config.getBackupCount(), config.getAsyncBackupCount(),
                config.getSplitBrainProtectionName(), config.getMergePolicyConfig());
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
     * Returns the split brain protection name for operations.
     *
     * @return the split brain protection name
     */
    public String getSplitBrainProtectionName() {
        return splitBrainProtectionName;
    }

    /**
     * Sets the split brain protection name for operations.
     *
     * @param splitBrainProtectionName the split brain protection name
     * @return the updated configuration
     */
    public CardinalityEstimatorConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
        this.splitBrainProtectionName = splitBrainProtectionName;
        return this;
    }


    @Override
    public String toString() {
        return "CardinalityEstimatorConfig{" + "name='" + name + '\'' + ", backupCount=" + backupCount
                + ", asyncBackupCount=" + asyncBackupCount + ", readOnly=" + readOnly + ", splitBrainProtectionName="
                + splitBrainProtectionName + ", mergePolicyConfig=" + mergePolicyConfig + '}';
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
    public int getClassId() {
        return ConfigDataSerializerHook.CARDINALITY_ESTIMATOR_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);
        out.writeUTF(splitBrainProtectionName);
        out.writeObject(mergePolicyConfig);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();
        splitBrainProtectionName = in.readUTF();
        mergePolicyConfig = in.readObject();
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
        if (splitBrainProtectionName != null ? !splitBrainProtectionName.equals(that.splitBrainProtectionName)
                : that.splitBrainProtectionName != null) {
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
        result = 31 * result + (splitBrainProtectionName != null ? splitBrainProtectionName.hashCode() : 0);
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
        public CardinalityEstimatorConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
            throw new UnsupportedOperationException("This config is read-only cardinality estimator: " + getName());
        }

        @Override
        public CardinalityEstimatorConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
            throw new UnsupportedOperationException("This config is read-only cardinality estimator: " + getName());
        }
    }
}
