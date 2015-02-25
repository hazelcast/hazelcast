/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.io.Serializable;

/**
 * Configuration for map's capacity.
 * You can set a limit for number of entries or total memory cost of entries.
 */
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
         * Decide maximum entry count according to node
         */
        PER_NODE,
        /**
         * Decide maximum entry count according to partition
         */
        PER_PARTITION,
        /**
         * Decide maximum size with use heap percentage
         */
        USED_HEAP_PERCENTAGE,
        /**
         * Decide maximum size with use heap size
         */
        USED_HEAP_SIZE,
        /**
         * Decide minimum free heap percentage to trigger cleanup
         */
        FREE_HEAP_PERCENTAGE,
        /**
         * Decide minimum free heap size to trigger cleanup
         */
        FREE_HEAP_SIZE
    }

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
}
