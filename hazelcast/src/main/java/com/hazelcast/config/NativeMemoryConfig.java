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

import com.hazelcast.map.IMap;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.NativeOutOfMemoryError;

import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.Preconditions.isNotNull;

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
    private MemorySize size = new MemorySize(INITIAL_MEMORY_SIZE, MemoryUnit.MEGABYTES);
    private MemoryAllocatorType allocatorType = MemoryAllocatorType.POOLED;

    private int minBlockSize = DEFAULT_MIN_BLOCK_SIZE;
    private int pageSize = DEFAULT_PAGE_SIZE;
    private float metadataSpacePercentage = DEFAULT_METADATA_SPACE_PERCENTAGE;
    /**
     * Path to the non-volatile memory directory. {@code null} indicates the
     * standard RAM is used.
     */
    private String persistentMemoryDirectory;

    public NativeMemoryConfig() {
    }

    public NativeMemoryConfig(NativeMemoryConfig nativeMemoryConfig) {
        enabled = nativeMemoryConfig.enabled;
        size = nativeMemoryConfig.size;
        allocatorType = nativeMemoryConfig.allocatorType;
        minBlockSize = nativeMemoryConfig.minBlockSize;
        pageSize = nativeMemoryConfig.pageSize;
        metadataSpacePercentage = nativeMemoryConfig.metadataSpacePercentage;
        persistentMemoryDirectory = nativeMemoryConfig.persistentMemoryDirectory;
    }

    /**
     * Returns size of the native memory region.
     */
    public MemorySize getSize() {
        return size;
    }

    /**
     * Sets size of the native memory region.
     * <p>
     * Total size of the memory blocks allocated in native memory region cannot exceed this memory size.
     * When native memory region is completely allocated and in-use, further allocation requests will fail
     * with {@link NativeOutOfMemoryError}.
     *
     * @param size memory size
     * @return this {@link NativeMemoryConfig} instance
     */
    public NativeMemoryConfig setSize(MemorySize size) {
        this.size = isNotNull(size, "size");
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
        this.minBlockSize = checkPositive(minBlockSize, "Minimum block size should be positive");
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
        this.pageSize = checkPositive(pageSize, "Page size should be positive");
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
     * Returns the persistent memory directory (e.g. Intel Optane) to be used to store memory structures allocated by native
     * memory manager.
     * <p>
     * Default value is {@code null}. It indicates that volatile RAM is being used.
     * {@code null}
     */
    public String getPersistentMemoryDirectory() {
        return persistentMemoryDirectory;
    }

    /**
     * Sets the persistent memory directory (e.g. Intel Optane) to be used to store memory structures allocated by native memory
     * manager.
     * @param directory the persistent memory directory
     * @return this {@link NativeMemoryConfig} instance
     */
    public NativeMemoryConfig setPersistentMemoryDirectory(String directory) {
        this.persistentMemoryDirectory = directory;
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
        if (o == null || !(o instanceof NativeMemoryConfig)) {
            return false;
        }

        NativeMemoryConfig that = (NativeMemoryConfig) o;

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
        if (size != null ? !size.equals(that.size) : that.size != null) {
            return false;
        }
        if (persistentMemoryDirectory != null ? !persistentMemoryDirectory.equals(that.persistentMemoryDirectory)
                : that.persistentMemoryDirectory != null) {
            return false;
        }
        return allocatorType == that.allocatorType;
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (size != null ? size.hashCode() : 0);
        result = 31 * result + (allocatorType != null ? allocatorType.hashCode() : 0);
        result = 31 * result + minBlockSize;
        result = 31 * result + pageSize;
        result = 31 * result + (metadataSpacePercentage != +0.0f ? Float.floatToIntBits(metadataSpacePercentage) : 0);
        result = 31 * result + (persistentMemoryDirectory != null ? persistentMemoryDirectory.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "NativeMemoryConfig{"
                + "enabled=" + enabled
                + ", size=" + size
                + ", allocatorType=" + allocatorType
                + ", minBlockSize=" + minBlockSize
                + ", pageSize=" + pageSize
                + ", metadataSpacePercentage=" + metadataSpacePercentage
                + ", persistentMemoryDirectory=" + persistentMemoryDirectory
                + '}';
    }
}
