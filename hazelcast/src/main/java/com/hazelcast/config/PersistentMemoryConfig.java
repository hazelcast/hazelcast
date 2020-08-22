/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Configuration class for persistent memory devices (e.g. Intel Optane).
 */
public class PersistentMemoryConfig {
    /**
     * Paths to the non-volatile memory directory.
     */
    private List<PersistentMemoryDirectoryConfig> directoryConfigs = new LinkedList<>();

    public PersistentMemoryConfig() {
    }

    /**
     * Constructs an instance with copying the fields of the provided
     * {@link PersistentMemoryConfig}.
     *
     * @param persistentMemoryConfig The configuration to copy
     * @throws NullPointerException if {@code persistentMemoryConfig} is {@code null}
     */
    public PersistentMemoryConfig(@Nonnull PersistentMemoryConfig persistentMemoryConfig) {
        requireNonNull(persistentMemoryConfig).directoryConfigs
                .forEach(directoryConfig -> addDirectoryConfig(new PersistentMemoryDirectoryConfig(directoryConfig)));
    }

    /**
     * Returns the persistent memory directory configurations to be used
     * to store memory structures allocated by native memory manager.
     * <p>
     * By default there are no configuration is set indicating that
     * volatile RAM is being used.
     *
     * @return the list of the persistent memory directory configurations
     */
    @Nonnull
    public List<PersistentMemoryDirectoryConfig> getDirectoryConfigs() {
        return directoryConfigs;
    }

    /**
     * Sets the persistent memory directory configuration to the set of
     * directories provided in the {@code directoryConfigs} argument.
     * <p/>
     * If the specified directories are not unique either in the directories
     * themselves or in the NUMA nodes specified for them,
     * {@link InvalidConfigurationException} is thrown. Setting the NUMA
     * node on the subset of the configured directories while leaving
     * not set on others also results in {@link InvalidConfigurationException}.
     *
     * @param directoryConfigs The persistent memory directories to set
     * @return this {@link PersistentMemoryConfig} instance
     * @throws InvalidConfigurationException If the configured directories
     *                                       violate consistency or
     *                                       uniqueness checks.
     * @throws NullPointerException if {@code directoryConfigs} is {@code null}
     */
    public PersistentMemoryConfig setDirectoryConfigs(@Nonnull List<PersistentMemoryDirectoryConfig> directoryConfigs) {
        ArrayList<PersistentMemoryDirectoryConfig> checkedConfigs = new ArrayList<>(requireNonNull(directoryConfigs).size());
        for (PersistentMemoryDirectoryConfig configToCheck : directoryConfigs) {
            for (PersistentMemoryDirectoryConfig checkedConfig : checkedConfigs) {
                validateDirectoryConfig(configToCheck, checkedConfig);
            }
            checkedConfigs.add(configToCheck);
        }

        this.directoryConfigs = directoryConfigs;
        return this;
    }

    /**
     * Adds the persistent memory directory configuration to be used to
     * store memory structures allocated by native memory manager.
     * <p/>
     * If the specified directories are not unique either in the directories
     * themselves or in the NUMA nodes specified for them,
     * {@link InvalidConfigurationException} is thrown. Setting the NUMA
     * node on the subset of the configured directories while leaving
     * not set on others also results in {@link InvalidConfigurationException}.
     *
     * @param directoryConfig the persistent memory directory configuration
     * @return this {@link PersistentMemoryConfig} instance
     * @throws InvalidConfigurationException If the configured directories
     *                                       violate consistency or
     *                                       uniqueness checks.
     * @throws NullPointerException if {@code directoryConfigs} is {@code null}
     */
    public PersistentMemoryConfig addDirectoryConfig(@Nonnull PersistentMemoryDirectoryConfig directoryConfig) {
        requireNonNull(directoryConfig);
        for (PersistentMemoryDirectoryConfig existingConfig : this.directoryConfigs) {
            validateDirectoryConfig(directoryConfig, existingConfig);
        }

        this.directoryConfigs.add(directoryConfig);
        return this;
    }

    private void validateDirectoryConfig(PersistentMemoryDirectoryConfig directoryConfig,
                                         PersistentMemoryDirectoryConfig existingConfig) {
        if (existingConfig.getDirectory().equals(directoryConfig.getDirectory())) {
            throw new InvalidConfigurationException(
                    "Persistent directories must be unique. '" + directoryConfig.getDirectory() + "' is already set.");
        }

        if (existingConfig.isNumaNodeSet() != directoryConfig.isNumaNodeSet()) {
            throw new InvalidConfigurationException(
                    "NUMA node on all persistent memory directories should either be set or left unset. NUMA node settings for"
                            + " directories '" + directoryConfig.getDirectory() + "' and '" + existingConfig.getDirectory()
                            + "' are not consistent.");
        }

        if (directoryConfig.isNumaNodeSet() && existingConfig.getNumaNode() == directoryConfig.getNumaNode()) {
            throw new InvalidConfigurationException(
                    "NUMA node must be set uniquely on the persistent memory directories. " + directoryConfig.getDirectory()
                            + " and " + existingConfig.getDirectory() + " have the same NUMA node set.");
        }
    }

    void setDirectoryConfig(@Nonnull PersistentMemoryDirectoryConfig directoryConfig) {
        requireNonNull(directoryConfig);
        // method to support 4.0 API of NativeMemoryConfig
        this.directoryConfigs.clear();
        this.directoryConfigs.add(directoryConfig);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PersistentMemoryConfig that = (PersistentMemoryConfig) o;

        return Objects.equals(directoryConfigs, that.directoryConfigs);
    }

    @Override
    public int hashCode() {
        return directoryConfigs.hashCode();
    }

    @Override
    public String toString() {
        return "PersistentMemoryConfig{"
                + "directoryConfigs=" + directoryConfigs
                + '}';
    }
}
