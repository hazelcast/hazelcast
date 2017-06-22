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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.util.Preconditions.checkBackupCount;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Configuration options for the {@link com.hazelcast.cardinality.CardinalityEstimator}
 */
public class CardinalityEstimatorConfig implements IdentifiedDataSerializable {

    /**
     * The number of sync backups per estimator
     */
    public static final int DEFAULT_SYNC_BACKUP_COUNT = 1;

    /**
     * The number of async backups per estimator
     */
    public static final int DEFAULT_ASYNC_BACKUP_COUNT = 0;

    private String name = "default";

    private int backupCount = DEFAULT_SYNC_BACKUP_COUNT;

    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;

    private transient CardinalityEstimatorConfigReadOnly readOnly;

    public CardinalityEstimatorConfig() {
    }

    public CardinalityEstimatorConfig(String name) {
        this.name = name;
    }

    public CardinalityEstimatorConfig(String name, int backupCount, int asyncBackupCount) {
        this.name = name;
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
    }

    public CardinalityEstimatorConfig(CardinalityEstimatorConfig config) {
        this(config.getName(), config.getBackupCount(), config.getAsyncBackupCount());
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

    @Override
    public String toString() {
        return "CardinalityEstimatorConfig{" + "name='" + name + '\'' + ", backupCount=" + backupCount + ", asyncBackupCount="
                + asyncBackupCount + ", readOnly=" + readOnly + '}';
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
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CardinalityEstimatorConfig that = (CardinalityEstimatorConfig) o;
        if (backupCount != that.backupCount) {
            return false;
        }
        if (asyncBackupCount != that.asyncBackupCount) {
            return false;
        }
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + backupCount;
        result = 31 * result + asyncBackupCount;
        return result;
    }

    private static class CardinalityEstimatorConfigReadOnly extends CardinalityEstimatorConfig {

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
    }
}
