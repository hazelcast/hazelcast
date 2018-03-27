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

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.util.Preconditions.checkBackupCount;
import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Contains the configuration for an {@link com.hazelcast.core.ISemaphore}.
 */
public class SemaphoreConfig implements IdentifiedDataSerializable, Versioned {

    /**
     * Default synchronous backup count.
     */
    public static final int DEFAULT_SYNC_BACKUP_COUNT = 1;
    /**
     * Default asynchronous backup count.
     */
    public static final int DEFAULT_ASYNC_BACKUP_COUNT = 0;

    private String name;
    private int initialPermits;
    private int backupCount = DEFAULT_SYNC_BACKUP_COUNT;
    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;
    private transient SemaphoreConfigReadOnly readOnly;

    private String quorumName;

    /**
     * Creates a default configured {@link SemaphoreConfig}.
     */
    public SemaphoreConfig() {
    }

    /**
     * Creates a SemaphoreConfig by cloning another one.
     *
     * @param config the SemaphoreConfig to copy
     * @throws IllegalArgumentException if config is {@code null}
     */
    public SemaphoreConfig(SemaphoreConfig config) {
        isNotNull(config, "config");
        this.name = config.getName();
        this.initialPermits = config.getInitialPermits();
        this.backupCount = config.getBackupCount();
        this.asyncBackupCount = config.getAsyncBackupCount();
        this.quorumName = config.getQuorumName();
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public SemaphoreConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new SemaphoreConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Gets the name of the semaphore. If no name has been configured, {@code null} is returned.
     *
     * @return the name of the semaphore
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the semaphore.
     *
     * @param name the name of the semaphore
     * @return the updated SemaphoreConfig
     * @throws IllegalArgumentException if name is {@code null} or empty
     */
    public SemaphoreConfig setName(String name) {
        this.name = checkHasText(name, "name must contain text");
        return this;
    }

    /**
     * Gets the initial number of permits.
     *
     * @return the initial number of permits
     */
    public int getInitialPermits() {
        return initialPermits;
    }

    /**
     * Sets the initial number of permits. The initial number of permits can be 0; meaning that there is no permit.
     * It can also be a negative number, meaning that there is a shortage of permits.
     *
     * @param initialPermits the initial number of permits
     * @return the updated SemaphoreConfig
     */
    public SemaphoreConfig setInitialPermits(int initialPermits) {
        this.initialPermits = initialPermits;
        return this;
    }

    /**
     * Returns the number of synchronous backups.
     *
     * @return the number of synchronous backups
     * @see #setBackupCount(int)
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Sets the number of synchronous backups.
     *
     * @param backupCount the number of synchronous backups to set
     * @return the updated SemaphoreConfig
     * @throws IllegalArgumentException if backupCount smaller than 0,
     *                                  or larger than the maximum number of backup
     *                                  or the sum of the backups and async backups is larger
     *                                  than the maximum number of backups
     * @see #setAsyncBackupCount(int)
     * @see #getBackupCount()
     */
    public SemaphoreConfig setBackupCount(int backupCount) {
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Returns the number of asynchronous backups.
     *
     * @return the number of asynchronous backups
     * @see #setAsyncBackupCount(int)
     */
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Sets the number of asynchronous backups. 0 means no backups.
     *
     * @param asyncBackupCount the number of asynchronous synchronous backups to set
     * @return the updated SemaphoreConfig
     * @throws IllegalArgumentException if asyncBackupCount smaller than 0,
     *                                  or larger than the maximum number of backup
     *                                  or the sum of the backups and async backups is larger than the maximum
     *                                  number of backups
     * @see #setBackupCount(int)
     * @see #getAsyncBackupCount()
     */
    public SemaphoreConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Returns the total number of backups (synchronous plus asynchronous);
     * the returned value will always equal or bigger than 0.
     *
     * @return the total number of backups (synchronous plus asynchronous)
     */
    public int getTotalBackupCount() {
        return asyncBackupCount + backupCount;
    }

    public String getQuorumName() {
        return quorumName;
    }

    public SemaphoreConfig setQuorumName(String quorumName) {
        this.quorumName = quorumName;
        return this;
    }

    @Override
    public String toString() {
        return "SemaphoreConfig{"
                + "name='" + name + '\''
                + ", initialPermits=" + initialPermits
                + ", backupCount=" + backupCount
                + ", asyncBackupCount=" + asyncBackupCount
                + ", quorumName=" + quorumName
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.SEMAPHORE_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(initialPermits);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);
        // RU_COMPAT_3_9
        if (out.getVersion().isGreaterOrEqual(Versions.V3_10)) {
            out.writeUTF(quorumName);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        initialPermits = in.readInt();
        backupCount = in.readInt();
        asyncBackupCount = in.readInt();
        // RU_COMPAT_3_9
        if (in.getVersion().isGreaterOrEqual(Versions.V3_10)) {
            quorumName = in.readUTF();
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SemaphoreConfig)) {
            return false;
        }

        SemaphoreConfig that = (SemaphoreConfig) o;

        if (initialPermits != that.initialPermits) {
            return false;
        }
        if (backupCount != that.backupCount) {
            return false;
        }
        if (asyncBackupCount != that.asyncBackupCount) {
            return false;
        }
        if (quorumName != null ? !quorumName.equals(that.quorumName) : that.quorumName != null) {
            return false;
        }
        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public final int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + initialPermits;
        result = 31 * result + backupCount;
        result = 31 * result + asyncBackupCount;
        result = 31 * result + (quorumName != null ? quorumName.hashCode() : 0);
        return result;
    }
}
