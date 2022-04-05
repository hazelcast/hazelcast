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
import com.hazelcast.memory.Capacity;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Objects;

/**
 * Memory tier configuration of Tiered-Store.
 *
 * @since 5.1
 */
public class MemoryTierConfig implements IdentifiedDataSerializable {

    /**
     * Default capacity. It is 256 MB.
     */
    public static final Capacity DEFAULT_CAPACITY = Capacity.of(256, MemoryUnit.MEGABYTES);

    private Capacity capacity = DEFAULT_CAPACITY;

    public MemoryTierConfig() {

    }

    public MemoryTierConfig(MemoryTierConfig memoryTierConfig) {
        capacity = memoryTierConfig.getCapacity();
    }

    /**
     * Returns the capacity of this memory tier.
     *
     * @return memory tier capacity.
     */
    public Capacity getCapacity() {
        return capacity;
    }

    /**
     * Sets the capacity of this memory tier.
     *
     * @param capacity capacity.
     * @return this MemoryTierConfig
     */
    public MemoryTierConfig setCapacity(Capacity capacity) {
        this.capacity = capacity;
        return this;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MemoryTierConfig)) {
            return false;
        }

        MemoryTierConfig that = (MemoryTierConfig) o;

        return Objects.equals(capacity, that.capacity);
    }

    @Override
    public final int hashCode() {
        return capacity != null ? capacity.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "MemoryTierConfig{"
                + "capacity=" + capacity
                + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(capacity.getValue());
        out.writeString(capacity.getUnit().name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        capacity = Capacity.of(in.readLong(), MemoryUnit.valueOf(in.readString()));
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.MEMORY_TIER_CONFIG;
    }
}
