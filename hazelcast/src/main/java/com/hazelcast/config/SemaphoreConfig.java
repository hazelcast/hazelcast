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

import static com.hazelcast.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.util.Preconditions.checkBackupCount;
import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Contains the configuration for an {@link com.hazelcast.core.ISemaphore}.
 */
public class SemaphoreConfig {

    /**
     * Default synchronous backup count
     */
    public static final int DEFAULT_SYNC_BACKUP_COUNT = 1;
    /**
     * Default asynchronous backup count
     */
    public static final int DEFAULT_ASYNC_BACKUP_COUNT = 0;

    private String name;
    private int initialPermits;
    private int backupCount = DEFAULT_SYNC_BACKUP_COUNT;
    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;
    private SemaphoreConfigReadOnly readOnly;

    /**
     * Creates a default configured {@link SemaphoreConfig}.
     */
    public SemaphoreConfig() {
    }

    /**
     * Creates a SemaphoreConfig by cloning another one.
     *
     * @param config the SemaphoreConfig to copy
     * @throws IllegalArgumentException if config is null.
     */
    public SemaphoreConfig(SemaphoreConfig config) {
        isNotNull(config, "config");
        this.name = config.getName();
        this.initialPermits = config.getInitialPermits();
        this.backupCount = config.getBackupCount();
        this.asyncBackupCount = config.getAsyncBackupCount();
    }

    public SemaphoreConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new SemaphoreConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Gets the name of the semaphore. If no name has been configured, null is returned.
     *
     * @return the name of the semaphore.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the semaphore.
     *
     * @param name the name of the semaphore
     * @return the updated SemaphoreConfig
     * @throws IllegalArgumentException if name is null or empty.
     */
    public SemaphoreConfig setName(String name) {
        this.name = checkHasText(name, "name must contain text");
        return this;
    }

    /**
     * Gets the initial number of permits
     *
     * @return the initial number of permits.
     */
    public int getInitialPermits() {
        return initialPermits;
    }

    /**
     * Sets the initial number of permits. The initial number of permits can be 0; meaning that there is no permit but
     * it can also be negative meaning that there is a shortage of permits.
     *
     * @param initialPermits the initial number of permits.
     * @return the updated SemaphoreConfig
     */
    public SemaphoreConfig setInitialPermits(int initialPermits) {
        this.initialPermits = initialPermits;
        return this;
    }

    /**
     * Returns the number of synchronous backups.
     *
     * @return the number of synchronous backups.
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
     * @throws new IllegalArgumentException if backupCount smaller than 0,
     *             or larger than the maximum number of backup
     *             or the sum of the backups and async backups is larger than the maximum number of backups
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
     * @return the number of asynchronous backups.
     * @see #setAsyncBackupCount(int)
     */
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Sets the number of asynchronous backups. 0 means no backups
     *
     * @param asyncBackupCount the number of asynchronous synchronous backups to set
     * @return the updated SemaphoreConfig
     * @throws IllegalArgumentException if asyncBackupCount smaller than 0,
     *             or larger than the maximum number of backup
     *             or the sum of the backups and async backups is larger than the maximum number of backups
     * @see #setBackupCount(int)
     * @see #getAsyncBackupCount()
     */
    public SemaphoreConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Returns the total number of backups; the returned value will always equal or bigger than 0.
     *
     * @return the total number of backups.
     */
    public int getTotalBackupCount() {
        return asyncBackupCount + backupCount;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SemaphoreConfig{");
        sb.append("name='").append(name).append('\'');
        sb.append(", initialPermits=").append(initialPermits);
        sb.append(", backupCount=").append(backupCount);
        sb.append(", asyncBackupCount=").append(asyncBackupCount);
        sb.append('}');
        return sb.toString();
    }

    /**
     * Contains configuration for Semaphore(read only)
     */

    static class SemaphoreConfigReadOnly extends SemaphoreConfig {

        public SemaphoreConfigReadOnly(SemaphoreConfig config) {
            super(config);
        }

        public SemaphoreConfig setName(String name) {
            throw new UnsupportedOperationException("This config is read-only semaphore: " + getName());
        }

        public SemaphoreConfig setInitialPermits(int initialPermits) {
            throw new UnsupportedOperationException("This config is read-only semaphore: " + getName());
        }

        public SemaphoreConfig setBackupCount(int backupCount) {
            throw new UnsupportedOperationException("This config is read-only semaphore: " + getName());
        }

        public SemaphoreConfig setAsyncBackupCount(int asyncBackupCount) {
            throw new UnsupportedOperationException("This config is read-only semaphore: " + getName());
        }
    }
}
