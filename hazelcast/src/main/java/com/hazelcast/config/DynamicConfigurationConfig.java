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

import javax.annotation.Nonnull;
import java.io.File;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Dynamic Configuration related configuration options.
 * @since 5.1
 */
public class DynamicConfigurationConfig {

    /**
     * Default name of the folder where backups will be kept. Note that by
     * default this is name of the folder, and it will be in the node startup
     * directory.
     */
    public static final String DEFAULT_BACKUP_DIR = "dynamic-configuration-backups";

    /**
     * Default number of backup declarative configuration files to keep.
     */
    public static final int DEFAULT_BACKUP_COUNT = 5;

    private boolean persistenceEnabled;
    private File backupDir = new File(DEFAULT_BACKUP_DIR);
    private int backupCount = DEFAULT_BACKUP_COUNT;

    /**
     * Returns whether Dynamic Configuration Persistence is enabled on this member.
     *
     * @return true if Dynamic Configuration Persistence is enabled
     */
    public boolean isPersistenceEnabled() {
        return persistenceEnabled;
    }

    /**
     * Sets whether Dynamic Configuration Persistence is enabled on this member.
     *
     * @param persistenceEnabled true if enabled
     * @return DynamicConfigurationConfig
     */
    @Nonnull
    public DynamicConfigurationConfig setPersistenceEnabled(boolean persistenceEnabled) {
        this.persistenceEnabled = persistenceEnabled;
        return this;
    }

    /**
     * Returns backup directory where declarative configuration backups will be kept.
     *
     * @return backupDir
     */
    @Nonnull
    public File getBackupDir() {
        return backupDir;
    }

    /**
     * Sets backup directory where declarative configuration backups will be kept.
     *
     * @param backupDir can be absolute path or relative path to the node startup directory
     * @return DynamicConfigurationConfig
     */
    @Nonnull
    public DynamicConfigurationConfig setBackupDir(@Nonnull File backupDir) {
        checkNotNull(backupDir);
        this.backupDir = backupDir;
        return this;
    }

    /**
     * Returns backup count. Last {@code backupCount} backups will be kept.
     *
     * @return backupCount
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Returns backup count. Last {@code backupCount} backups will be kept.
     *
     * @param backupCount backup count
     * @return this DynamicConfigurationConfig
     */
    @Nonnull
    public DynamicConfigurationConfig setBackupCount(int backupCount) {
        this.backupCount = backupCount;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamicConfigurationConfig that = (DynamicConfigurationConfig) o;
        return persistenceEnabled == that.persistenceEnabled
                && backupCount == that.backupCount
                && Objects.equals(backupDir, that.backupDir);
    }

    @Override
    public int hashCode() {
        int result = (persistenceEnabled ? 1 : 0);
        result = 31 * result + (backupDir != null ? backupDir.hashCode() : 0);
        result = 31 * result + backupCount;
        return result;
    }

    @Override
    public String toString() {
        return "DynamicConfigurationConfig{"
                + "persistenceEnabled=" + persistenceEnabled
                + ", backupDir=" + backupDir
                + ", backupCount=" + backupCount
                + '}';
    }
}
