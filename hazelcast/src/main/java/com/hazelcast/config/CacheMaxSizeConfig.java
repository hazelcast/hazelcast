/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
 * Configuration for cache's capacity.
 * You can set a limit for number of entries or total memory cost of entries.
 */
public class CacheMaxSizeConfig implements DataSerializable, Serializable {

    /**
     * Default maximum size of cache.
     */
    public static final int DEFAULT_MAX_SIZE = Integer.MAX_VALUE;

    private CacheMaxSizeConfigReadOnly readOnly;

    private CacheMaxSizePolicy maxSizePolicy = CacheMaxSizePolicy.ENTRY_COUNT;

    private int size = DEFAULT_MAX_SIZE;

    public CacheMaxSizeConfig() {
    }

    public CacheMaxSizeConfig(int size, CacheMaxSizePolicy maxSizePolicy) {
        setSize(size);
        this.maxSizePolicy = maxSizePolicy;
    }

    public CacheMaxSizeConfig(CacheMaxSizeConfig config) {
        this.size = config.size;
        this.maxSizePolicy = config.maxSizePolicy;
    }

    /**
     * Maximum Size Policy
     */
    public enum CacheMaxSizePolicy {
        /**
         * Decide maximum entry count according to node
         */
        ENTRY_COUNT,
        /**
         * Decide maximum size with use native memory size
         */
        USED_NATIVE_MEMORY_SIZE,
        /**
         * Decide maximum size with use native memory percentage
         */
        USED_NATIVE_MEMORY_PERCENTAGE,
        /**
         * Decide minimum free native memory size to trigger cleanup
         */
        FREE_NATIVE_MEMORY_SIZE,
        /**
         * Decide minimum free native memory percentage to trigger cleanup
         */
        FREE_NATIVE_MEMORY_PERCENTAGE
    }

    public CacheMaxSizeConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new CacheMaxSizeConfigReadOnly(this);
        }
        return readOnly;
    }

    public int getSize() {
        return size;
    }

    public CacheMaxSizeConfig setSize(int size) {
        if (size > 0) {
            this.size = size;
        }
        return this;
    }

    public CacheMaxSizePolicy getMaxSizePolicy() {
        return maxSizePolicy;
    }

    public CacheMaxSizeConfig setMaxSizePolicy(CacheMaxSizePolicy maxSizePolicy) {
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
        maxSizePolicy = CacheMaxSizePolicy.valueOf(in.readUTF());
        size = in.readInt();
    }

    @Override
    public String toString() {
        return "CacheMaxSizeConfig{"
                + "maxSizePolicy='" + maxSizePolicy
                + '\''
                + ", size=" + size
                + '}';
    }
}
