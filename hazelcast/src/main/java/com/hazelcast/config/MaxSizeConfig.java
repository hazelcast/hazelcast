/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.BinaryInterface;

import java.io.IOException;
import java.io.Serializable;

/**
 * Configuration for map's capacity.
 * You can set a limit for number of entries or total memory cost of entries.
 */
@BinaryInterface
public class MaxSizeConfig implements DataSerializable, Serializable {

    /**
     * Default maximum size of map.
     */
    public static final int DEFAULT_MAX_SIZE = Integer.MAX_VALUE;

    private MaxSizeConfigReadOnly readOnly;

    private MaxSizePolicy maxSizePolicy = MaxSizePolicy.PER_NODE;

    private int size = DEFAULT_MAX_SIZE;

    public MaxSizeConfig() {
    }

    public MaxSizeConfig(int size, MaxSizePolicy maxSizePolicy) {
        setSize(size);
        this.maxSizePolicy = maxSizePolicy;
    }

    public MaxSizeConfig(MaxSizeConfig config) {
        this.size = config.size;
        this.maxSizePolicy = config.maxSizePolicy;
    }

    /**
     * Maximum Size Policy
     */
    public enum MaxSizePolicy {

        /**
         * Policy based on maximum number of entries stored per data structure (map, cache etc)
         * on each Hazelcast instance
         */
        PER_NODE,
        /**
         * Policy based on maximum number of entries stored per data structure (map, cache etc)
         * on each partition
         */
        PER_PARTITION,
        /**
         * Policy based on maximum used JVM heap memory percentage per data structure (map, cache etc)
         * on each Hazelcast instance
         */
        USED_HEAP_PERCENTAGE,
        /**
         * Policy based on maximum used JVM heap memory in megabytes per data structure (map, cache etc)
         * on each Hazelcast instance
         */
        USED_HEAP_SIZE,
        /**
         * Policy based on minimum free JVM heap memory percentage per JVM
         */
        FREE_HEAP_PERCENTAGE,
        /**
         * Policy based on minimum free JVM heap memory in megabytes per JVM
         */
        FREE_HEAP_SIZE,
        /**
         * Policy based on maximum used native memory in megabytes per data structure (map, cache etc)
         * on each Hazelcast instance
         */
        USED_NATIVE_MEMORY_SIZE,
        /**
         * Policy based on maximum used native memory percentage per data structure (map, cache etc)
         * on each Hazelcast instance
         */
        USED_NATIVE_MEMORY_PERCENTAGE,
        /**
         * Policy based on minimum free native memory in megabytes per Hazelcast instance
         */
        FREE_NATIVE_MEMORY_SIZE,
        /**
         * Policy based on minimum free native memory percentage per Hazelcast instance
         */
        FREE_NATIVE_MEMORY_PERCENTAGE
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public MaxSizeConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new MaxSizeConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Returns the size of the map.
     *
     * @return the size of the map
     */
    public int getSize() {
        return size;
    }

    /**
     * Sets the maximum size of the map.
     *
     * @param size the maximum size of the map
     * @return the map MaxSizeConfig
     */
    public MaxSizeConfig setSize(int size) {
        if (size > 0) {
            this.size = size;
        }
        return this;
    }

    /**
     * Returns the maximum size policy of the map.
     *
     * @return the MaxSizePolicy of the map
     */
    public MaxSizePolicy getMaxSizePolicy() {
        return maxSizePolicy;
    }

    /**
     * Ses the maximum size policy of the map.
     *
     * @param maxSizePolicy the maximum size policy to set for the map
     * @return this MaxSizeConfig
     */
    public MaxSizeConfig setMaxSizePolicy(MaxSizePolicy maxSizePolicy) {
        this.maxSizePolicy = maxSizePolicy;
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(maxSizePolicy.toString());
        out.writeInt(size);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        maxSizePolicy = MaxSizePolicy.valueOf(in.readUTF());
        size = in.readInt();
    }

    @Override
    public String toString() {
        return "MaxSizeConfig{"
                + "maxSizePolicy='" + maxSizePolicy
                + '\''
                + ", size=" + size
                + '}';
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MaxSizeConfig)) {
            return false;
        }

        MaxSizeConfig that = (MaxSizeConfig) o;
        if (size != that.size) {
            return false;
        }
        return maxSizePolicy == that.maxSizePolicy;
    }

    @Override
    public final int hashCode() {
        int result = maxSizePolicy != null ? maxSizePolicy.hashCode() : 0;
        result = 31 * result + size;
        return result;
    }
}
