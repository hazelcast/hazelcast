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

import java.util.Objects;

/**
 * Configuration class for directories that persistent memories are mounted at.
 */
public class PersistentMemoryDirectoryConfig {
    private static final int NUMA_NODE_UNSET = -1;

    private String directory;
    private int numaNode = NUMA_NODE_UNSET;

    /**
     * Creates an instance with the {@code directory} specified.
     *
     * @param directory The directory where the persistent memory is
     *                  mounted at
     */
    public PersistentMemoryDirectoryConfig(String directory) {
        this.directory = directory;
    }

    /**
     * Creates an instance with the {@code directory} and NUMA node specified.
     *
     * @param directory The directory where the persistent memory is
     *                  mounted at
     * @param numaNode  The NUMA node that the persistent memory mounted
     *                  to the given directory is attached to
     */
    public PersistentMemoryDirectoryConfig(String directory, int numaNode) {
        this.directory = directory;
        this.numaNode = numaNode;
    }

    /**
     * Constructs an instance by copying the provided {@link PersistentMemoryDirectoryConfig}.
     *
     * @param directoryConfig The configuration to copy
     */
    public PersistentMemoryDirectoryConfig(PersistentMemoryDirectoryConfig directoryConfig) {
        this.directory = directoryConfig.directory;
        this.numaNode = directoryConfig.numaNode;
    }

    /**
     * Returns the directory of this {@link PersistentMemoryDirectoryConfig}.
     *
     * @return the directory
     */
    public String getDirectory() {
        return directory;
    }

    /**
     * Returns the {@code directory} of this {@link PersistentMemoryDirectoryConfig}.
     *
     * @param directory the directory to set
     */
    public void setDirectory(String directory) {
        this.directory = directory;
    }

    /**
     * Returns the NUMA node the persistent memory mounted to the given
     * directory is attached to.
     *
     * @return the NUMA node of the persistent memory
     */
    public int getNumaNode() {
        return numaNode;
    }

    /**
     * Sets the NUMA node the persistent memory mounted to the given
     * directory is attached to.
     *
     * @param numaNode the NUMA node to set
     */
    public void setNumaNode(int numaNode) {
        this.numaNode = numaNode;
    }

    /**
     * Returns if the NUMA node for the given persistent memory directory
     * is set.
     *
     * @return {@code true} if the NUMA node is set, {@code false} otherwise
     */
    public boolean isNumaNodeSet() {
        return numaNode != NUMA_NODE_UNSET;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PersistentMemoryDirectoryConfig that = (PersistentMemoryDirectoryConfig) o;

        if (numaNode != that.numaNode) {
            return false;
        }
        return Objects.equals(directory, that.directory);
    }

    @Override
    public int hashCode() {
        int result = directory != null ? directory.hashCode() : 0;
        result = 31 * result + numaNode;
        return result;
    }

    @Override
    public String toString() {
        return "PersistentMemoryDirectoryConfig{"
                + "directory='" + directory + '\''
                + ", numaNode=" + numaNode
                + '}';
    }
}
