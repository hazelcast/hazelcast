/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Objects;

/**
 * In-Memory tier configuration of Tiered-Store.
 *
 * @since 5.1
 */
public class TSInMemoryTierConfig implements IdentifiedDataSerializable {

    /**
     * Default capacity in bytes.
     */
    public static final long DEFAULT_CAPACITY = 256L << 20;

    private long capacity = DEFAULT_CAPACITY;

    public TSInMemoryTierConfig() {

    }

    public TSInMemoryTierConfig(TSInMemoryTierConfig tsInMemoryTierConfig) {
        capacity = tsInMemoryTierConfig.getCapacity();
    }

    /**
     * Returns the capacity of this in-memory tier.
     *
     * @return in-memory tier capacity.
     */
    public long getCapacity() {
        return capacity;
    }

    /**
     * Sets the capacity of this in-memory tier.
     *
     * @param capacity capacity.
     * @return this TSInMemoryTierConfig
     */
    public TSInMemoryTierConfig setCapacity(long capacity) {
        this.capacity = capacity;
        return this;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TSInMemoryTierConfig)) {
            return false;
        }

        TSInMemoryTierConfig that = (TSInMemoryTierConfig) o;

        return Objects.equals(capacity, that.capacity);
    }

    @Override
    public final int hashCode() {
        return (int) (capacity ^ (capacity >>> 32));
    }

    @Override
    public String toString() {
        return "TSInMemoryTierConfig{"
                + "capacity=" + capacity
                + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(capacity);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        capacity = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.TS_IN_MEMORY_TIER_CONFIG;
    }
}
