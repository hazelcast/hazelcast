/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;

import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Contains native memory configuration.
 */
public class NativeMemoryConfig {

    /**
     * Default minimum block size
     */
    public static final int DEFAULT_MIN_BLOCK_SIZE = 16;
    /**
     * Default page size
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

    public MemorySize getSize() {
        return size;
    }

    public NativeMemoryConfig setSize(final MemorySize size) {
        this.size = isNotNull(size, "Memory size");
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public NativeMemoryConfig setEnabled(final boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public MemoryAllocatorType getAllocatorType() {
        return allocatorType;
    }

    public NativeMemoryConfig setAllocatorType(MemoryAllocatorType allocatorType) {
        this.allocatorType = allocatorType;
        return this;
    }

    public int getMinBlockSize() {
        return minBlockSize;
    }

    public NativeMemoryConfig setMinBlockSize(int minBlockSize) {
        this.minBlockSize = checkPositive(minBlockSize, "Minimum block size should be positive");
        return this;
    }

    public int getPageSize() {
        return pageSize;
    }

    public NativeMemoryConfig setPageSize(int pageSize) {
        this.pageSize = checkPositive(pageSize, "Page size should be positive");
        return this;
    }

    public float getMetadataSpacePercentage() {
        return metadataSpacePercentage;
    }

    public NativeMemoryConfig setMetadataSpacePercentage(float metadataSpacePercentage) {
        this.metadataSpacePercentage = metadataSpacePercentage;
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
    public String toString() {
        return "NativeMemoryConfig{"
                + "enabled=" + enabled
                + ", size=" + size
                + ", allocatorType=" + allocatorType
                + '}';
    }
}
