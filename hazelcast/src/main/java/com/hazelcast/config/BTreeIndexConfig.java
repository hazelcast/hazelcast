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

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Configuration options specific for B-Tree indexes.
 */
public class BTreeIndexConfig implements IdentifiedDataSerializable {

    public static final int DEFAULT_PAGE_SIZE = 1024; // todo HZ-1272 think of correct values
    public static final int DEFAULT_IN_MEMORY_REGION_SIZE = 1024; // todo HZ-1272 think of correct values

    private int pageSize = DEFAULT_PAGE_SIZE;
    private int inMemoryRegionSize = DEFAULT_IN_MEMORY_REGION_SIZE;

    private MemoryTierConfig memoryTierConfig;


    public BTreeIndexConfig() {
    }

    public BTreeIndexConfig(BTreeIndexConfig other) {
        this.pageSize = other.pageSize;
        this.inMemoryRegionSize = other.inMemoryRegionSize;
        this.memoryTierConfig = other.memoryTierConfig == null ? null : new MemoryTierConfig(other.memoryTierConfig);
    }

    /**
     * Returns the page size of B-Tree index.
     * @return index page size.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Sets index page size to given non-negative value.
     * @param pageSize page size of the index
     */
    public void setPageSize(int pageSize) {
        Preconditions.checkNotNegative(pageSize, "page size cannot be negative, but is: " + pageSize);
        this.pageSize = pageSize;
    }

    /**
     * Returns in-memory region size of this B-Tree index.
     * @return in-memory region size.
     */
    public int getInMemoryRegionSize() {
        return inMemoryRegionSize;
    }

    public void setInMemoryRegionSize(int inMemoryRegionSize) {
        this.inMemoryRegionSize = inMemoryRegionSize;
    }

    /**
     * Returns memory tier configuration for this index.
     * @return memory tier configuration for this index.
     */
    @Nonnull
    public MemoryTierConfig getMemoryTierConfig() {
        if (memoryTierConfig == null) {
            memoryTierConfig = new MemoryTierConfig();
        }
        return memoryTierConfig;
    }

    /**
     * Sets memory tier configuration for this index to given configuration.
     * @param memoryTierConfig new memory tier configuration to be set.
     */
    public void setMemoryTierConfig(MemoryTierConfig memoryTierConfig) {
        this.memoryTierConfig = memoryTierConfig;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(pageSize);
        out.writeInt(inMemoryRegionSize);
        out.writeObject(memoryTierConfig);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        pageSize = in.readInt();
        inMemoryRegionSize = in.readInt();
        memoryTierConfig = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.BTREE_INDEX_CONFIG;
    }
}
