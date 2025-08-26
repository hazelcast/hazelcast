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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.map.IMap;
import com.hazelcast.memory.Capacity;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.NativeOutOfMemoryError;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.Preconditions.isNotNull;
import static java.util.Objects.requireNonNull;

/**
 * Configures native memory region.
 * <p>
 * Native memory is allocated outside JVM heap space and is not subject to JVM garbage collection.
 * Therefore, hundreds of gigabytes of native memory can be allocated &amp; used without introducing
 * pressure on GC mechanism.
 * <p>
 * Data structures, such as {@link IMap} and {@link com.hazelcast.cache.ICache},
 * store their data (entries, indexes etc.) in native memory region when they are configured with
 * {@link InMemoryFormat#NATIVE}.
 */
public class NativeMemoryConfig {

    /**
     * Default minimum block size in bytes
     */
    public static final int DEFAULT_MIN_BLOCK_SIZE = 16;
    /**
     * Default page size in bytes
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int DEFAULT_PAGE_SIZE = 1 << 22;
    /**
     * Default metadata space percentage
     */
    public static final float DEFAULT_METADATA_SPACE_PERCENTAGE = 12.5f;
    /**
     * Minimum initial memory size in megabytes
     */
    public static final int MIN_INITIAL_MEMORY_SIZE = 512;
    /**
     * Initial memory size in megabytes
     */
    public static final int INITIAL_MEMORY_SIZE = MIN_INITIAL_MEMORY_SIZE;

    private boolean enabled;
    private Capacity capacity = new Capacity(INITIAL_MEMORY_SIZE, MemoryUnit.MEGABYTES);
    private MemoryAllocatorType allocatorType = MemoryAllocatorType.POOLED;

    private int minBlockSize = DEFAULT_MIN_BLOCK_SIZE;
    private int pageSize = DEFAULT_PAGE_SIZE;
    private float metadataSpacePercentage = DEFAULT_METADATA_SPACE_PERCENTAGE;

    private PersistentMemoryConfig persistentMemoryConfig = new PersistentMemoryConfig();

    public NativeMemoryConfig() {
    }

    public NativeMemoryConfig(NativeMemoryConfig nativeMemoryConfig) {
        enabled = nativeMemoryConfig.enabled;
        capacity = nativeMemoryConfig.capacity;
        allocatorType = nativeMemoryConfig.allocatorType;
        minBlockSize = nativeMemoryConfig.minBlockSize;
        pageSize = nativeMemoryConfig.pageSize;
        metadataSpacePercentage = nativeMemoryConfig.metadataSpacePercentage;
        persistentMemoryConfig = new PersistentMemoryConfig(nativeMemoryConfig.persistentMemoryConfig);
    }

    /**
     * Returns size of the native memory region.
     * @deprecated use {@link #getCapacity()} instead.
     */
    @Deprecated(since = "5.2")
    public MemorySize getSize() {
        return new MemorySize(capacity.getValue(), capacity.getUnit());
    }

    /**
     * Sets size of the native memory region.
     * <p>
     * Total size of the memory blocks allocated in native memory region cannot exceed this memory size.
     * When native memory region is completely allocated and in-use, further allocation requests will fail
     * with {@link NativeOutOfMemoryError}.
     *
     * @param capacity memory size
     * @return this {@link NativeMemoryConfig} instance
     * @deprecated use {@link #setCapacity(Capacity)} instead.
     */
    @Deprecated(since = "5.2")
    public NativeMemoryConfig setSize(MemorySize capacity) {
        this.capacity = isNotNull(capacity, "size");
        return this;
    }

    /**
     * Returns size (capacity) of the native memory region.
     */
    public Capacity getCapacity() {
        return capacity;
    }

    /**
     * Sets size (capacity) of the native memory region.
     * <p>
     * Total capacity of the memory blocks allocated in native memory region cannot exceed this memory size.
     * When native memory region is completely allocated and in-use, further allocation requests will fail
     * with {@link NativeOutOfMemoryError}.
     *
     * @param capacity memory size
     * @return this {@link NativeMemoryConfig} instance
     */
    public NativeMemoryConfig setCapacity(Capacity capacity) {
        this.capacity = isNotNull(capacity, "capacity");
        return this;
    }

    /**
     * Returns {@code true} if native memory allocation is enabled, {@code false} otherwise.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables or disables native memory allocation.
     *
     * @return this {@link NativeMemoryConfig} instance
     */
    public NativeMemoryConfig setEnabled(final boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Returns the {@link MemoryAllocatorType} to be used while allocating native memory.
     */
    public MemoryAllocatorType getAllocatorType() {
        return allocatorType;
    }

    /**
     * Sets the {@link MemoryAllocatorType} to be used while allocating native memory.
     *
     * @param allocatorType {@code MemoryAllocatorType}
     * @return this {@link NativeMemoryConfig} instance
     */
    public NativeMemoryConfig setAllocatorType(MemoryAllocatorType allocatorType) {
        this.allocatorType = allocatorType;
        return this;
    }

    /**
     * Returns the minimum memory block size, in bytes, to be served by native memory manager.
     * Allocation requests smaller than minimum block size are served with the minimum block size.
     * Default value is {@link #DEFAULT_MIN_BLOCK_SIZE} bytes.
     * <p>
     * <strong>This configuration is used only by {@link MemoryAllocatorType#POOLED}, otherwise ignored.</strong>
     */
    public int getMinBlockSize() {
        return minBlockSize;
    }

    /**
     * Sets the minimum memory block size, in bytes, to be served by native memory manager.
     * Allocation requests smaller than minimum block size are served with the minimum block size.
     * <p>
     * <strong>This configuration is used only by {@link MemoryAllocatorType#POOLED}, otherwise ignored.</strong>
     *
     * @param minBlockSize minimum memory block size
     * @return this {@link NativeMemoryConfig} instance
     */
    public NativeMemoryConfig setMinBlockSize(int minBlockSize) {
        this.minBlockSize = checkPositive("minBlockSize", minBlockSize);
        return this;
    }

    /**
     * Returns the page size, in bytes, to be allocated by native memory manager as a single block. These page blocks are
     * split into smaller blocks to serve allocation requests.
     * Allocation requests greater than the page size are allocated from system directly,
     * instead of managed memory pool.
     * Default value is {@link #DEFAULT_PAGE_SIZE} bytes.
     * <p>
     * <strong>This configuration is used only by {@link MemoryAllocatorType#POOLED}, otherwise ignored.</strong>
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Sets the page size, in bytes, to be allocated by native memory manager as a single block. These page blocks are
     * split into smaller blocks to serve allocation requests.
     * Allocation requests greater than the page size are allocated from system directly,
     * instead of managed memory pool.
     * <p>
     * <strong>This configuration is used only by {@link MemoryAllocatorType#POOLED}, otherwise ignored.</strong>
     *
     * @param pageSize size of the page
     * @return this {@link NativeMemoryConfig} instance
     */
    public NativeMemoryConfig setPageSize(int pageSize) {
        this.pageSize = checkPositive("pageSize", pageSize);
        return this;
    }

    /**
     * Returns the percentage of native memory space to be used to store metadata and internal memory structures
     * by the native memory manager.
     * Default value is {@link #DEFAULT_METADATA_SPACE_PERCENTAGE}.
     * <p>
     * <strong>This configuration is used only by {@link MemoryAllocatorType#POOLED}, otherwise ignored.</strong>
     */
    public float getMetadataSpacePercentage() {
        return metadataSpacePercentage;
    }

    /**
     * Sets the percentage of native memory space to be used to store metadata and internal memory structures
     * by the native memory manager.
     * <p>
     * <strong>This configuration is used only by {@link MemoryAllocatorType#POOLED}, otherwise ignored.</strong>
     *
     * @param metadataSpacePercentage percentage of metadata space
     * @return this {@link NativeMemoryConfig} instance
     */
    public NativeMemoryConfig setMetadataSpacePercentage(float metadataSpacePercentage) {
        this.metadataSpacePercentage = metadataSpacePercentage;
        return this;
    }

    /**
     * Returns the persistent memory configuration this native memory
     * configuration uses.
     *
     * @return the persistent memory configuration
     */
    @Nonnull
    public PersistentMemoryConfig getPersistentMemoryConfig() {
        return persistentMemoryConfig;
    }

    /**
     * Sets the persistent memory configuration this native memory
     * configuration uses.
     *
     * @param persistentMemoryConfig The persistent memory configuration to use
     */
    public void setPersistentMemoryConfig(@Nonnull PersistentMemoryConfig persistentMemoryConfig) {
        this.persistentMemoryConfig = requireNonNull(persistentMemoryConfig);
    }

    /**
     * Returns the persistent memory directory (e.g. Intel Optane) to be
     * used to store memory structures allocated by native memory manager.
     * If there are multiple persistent memory directories are defined in
     * {@link #persistentMemoryConfig}, an {@link IllegalStateException}
     * is thrown.
     *
     * @see PersistentMemoryConfig#getDirectoryConfigs()
     * @deprecated multiple persistent memory directories are
     * supported. Please use {@link PersistentMemoryConfig#getDirectoryConfigs()}
     * instead.
     */
    @Deprecated(since = "4.1")
    @Nullable
    public String getPersistentMemoryDirectory() {
        List<PersistentMemoryDirectoryConfig> directoryConfigs = persistentMemoryConfig.getDirectoryConfigs();
        int directoriesDefined = directoryConfigs.size();
        if (directoriesDefined > 1) {
            throw new HazelcastException("There are multiple persistent memory directories configured. Please use "
                    + "PersistentMemoryConfig.getDirectoryConfigs()!");
        }

        return directoriesDefined == 1 ? directoryConfigs.get(0).getDirectory() : null;
    }

    /**
     * Sets the persistent memory directory (e.g. Intel Optane) to be used
     * to store memory structures allocated by native memory manager. If
     * the {@link #persistentMemoryConfig} already contains directory
     * definition, it is overridden with the provided {@code directory}.
     *
     * @param directory the persistent memory directory
     * @return this {@link NativeMemoryConfig} instance
     * @see #getPersistentMemoryConfig()
     * @see PersistentMemoryConfig#addDirectoryConfig(PersistentMemoryDirectoryConfig)
     * @deprecated multiple persistent memory directories are
     * supported. Please use {@link #setPersistentMemoryConfig(PersistentMemoryConfig)}
     * or {@link PersistentMemoryConfig#addDirectoryConfig(PersistentMemoryDirectoryConfig)}
     * instead.
     */
    @Nonnull
    @Deprecated(since = "4.1")
    public NativeMemoryConfig setPersistentMemoryDirectory(@Nullable String directory) {
        if (directory != null) {
            this.persistentMemoryConfig.setDirectoryConfig(new PersistentMemoryDirectoryConfig(directory));
        } else {
            this.persistentMemoryConfig.getDirectoryConfigs().clear();
        }
        return this;
    }

    /**
     * Type of memory allocator:
     * <ul>
     * <li>STANDARD: allocate/free memory using default OS memory manager</li>
     * <li>POOLED: manage memory blocks in pool</li>
     * </ul>
     */
    public enum MemoryAllocatorType {
        /**
         * STANDARD memory allocator: allocate/free memory using default OS memory manager
         */
        STANDARD,

        /**
         * POOLED memory allocator: manage memory blocks in pool
         */
        POOLED
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NativeMemoryConfig that)) {
            return false;
        }

        if (enabled != that.enabled) {
            return false;
        }
        if (minBlockSize != that.minBlockSize) {
            return false;
        }
        if (pageSize != that.pageSize) {
            return false;
        }
        if (Float.compare(that.metadataSpacePercentage, metadataSpacePercentage) != 0) {
            return false;
        }
        if (!Objects.equals(capacity, that.capacity)) {
            return false;
        }
        if (!persistentMemoryConfig.equals(that.persistentMemoryConfig)) {
            return false;
        }
        return allocatorType == that.allocatorType;
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (capacity != null ? capacity.hashCode() : 0);
        result = 31 * result + (allocatorType != null ? allocatorType.hashCode() : 0);
        result = 31 * result + minBlockSize;
        result = 31 * result + pageSize;
        result = 31 * result + (metadataSpacePercentage != +0.0f ? Float.floatToIntBits(metadataSpacePercentage) : 0);
        result = 31 * result + (persistentMemoryConfig.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return "NativeMemoryConfig{"
                + "enabled=" + enabled
                + ", size=" + capacity
                + ", allocatorType=" + allocatorType
                + ", minBlockSize=" + minBlockSize
                + ", pageSize=" + pageSize
                + ", metadataSpacePercentage=" + metadataSpacePercentage
                + ", persistentMemoryConfig=" + persistentMemoryConfig
                + '}';
    }
}
