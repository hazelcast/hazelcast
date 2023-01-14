/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.memory.Capacity;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Objects;

import static com.hazelcast.memory.MemoryUnit.KILOBYTES;
import static java.util.Objects.requireNonNull;

/**
 * Configuration options specific for B-Tree indexes.
 *
 * @since 5.2
 */
public class BTreeIndexConfig implements IdentifiedDataSerializable {

    /**
     * Default value of page size for B-Trees.
     */
    public static final Capacity DEFAULT_PAGE_SIZE = Capacity.of(16, KILOBYTES);

    private Capacity pageSize = DEFAULT_PAGE_SIZE;

    private MemoryTierConfig memoryTierConfig = new MemoryTierConfig();


    public BTreeIndexConfig() {
    }

    public BTreeIndexConfig(BTreeIndexConfig other) {
        this.pageSize = other.pageSize;
        this.memoryTierConfig = other.memoryTierConfig == null ? null : new MemoryTierConfig(other.memoryTierConfig);
    }

    /**
     * Returns the page size of B-Tree index.
     * @return index page size.
     */
    public Capacity getPageSize() {
        return pageSize;
    }

    /**
     * Sets index page size to given non-negative value.
     * @param pageSize page size of the index
     */
    public BTreeIndexConfig setPageSize(Capacity pageSize) {
        this.pageSize = requireNonNull(pageSize, "pageSize cannot be null");
        return this;
    }

    /**
     * Returns memory tier configuration for this index.
     * @return memory tier configuration for this index.
     */
    @Nonnull
    public MemoryTierConfig getMemoryTierConfig() {
        return memoryTierConfig;
    }

    /**
     * Sets memory tier configuration for this index to given configuration.
     * @param memoryTierConfig new memory tier configuration to be set.
     */
    public BTreeIndexConfig setMemoryTierConfig(MemoryTierConfig memoryTierConfig) {
        this.memoryTierConfig = requireNonNull(memoryTierConfig, "memoryTierConfig cannot be null");
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(pageSize.getValue());
        out.writeString(pageSize.getUnit().name());
        out.writeObject(memoryTierConfig);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        long size = in.readLong();
        MemoryUnit unit = MemoryUnit.valueOf(in.readString());
        pageSize = Capacity.of(size, unit);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BTreeIndexConfig that = (BTreeIndexConfig) o;
        return Objects.equals(getPageSize(), that.getPageSize())
                && Objects.equals(getMemoryTierConfig(), that.getMemoryTierConfig());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPageSize(), getMemoryTierConfig());
    }

    @Override
    public String toString() {
        return "BTreeIndexConfig{"
                + "pageSize=" + getPageSize()
                + ", memoryTierConfig=" + getMemoryTierConfig()
                + '}';
    }
}
