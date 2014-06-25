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
/**
 * Configuration for map's capacity.
 * You can set a limit for number of entries or total memory cost of entries.
 */
public class MaxSizeConfig {

    private MaxSizeConfigReadOnly readOnly;

    private int size = MapConfig.DEFAULT_MAX_SIZE;
    private MaxSizePolicy maxSizePolicy = MaxSizePolicy.PER_NODE;

    public MaxSizeConfig() {
    }

    public MaxSizeConfig(int size, MaxSizePolicy maxSizePolicy) {
        this.size = size;
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
         * Decide maximum size of map according to node
         */
        PER_NODE,
        /**
         * Decide maximum size of map according to partition
         */
        PER_PARTITION,
        /**
         * Decide maximum size of map with use heap percentage
         */
        USED_HEAP_PERCENTAGE,
        /**
         * Decide maximum size of map with use heap size
         */
        USED_HEAP_SIZE
    }

    public MaxSizeConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new MaxSizeConfigReadOnly(this);
        }
        return readOnly;
    }

    public int getSize() {
        return size;
    }

    public MaxSizeConfig setSize(int size) {
        int paramSize = size;
        if (paramSize <= 0) {
            paramSize = Integer.MAX_VALUE;
        }
        this.size = paramSize;
        return this;
    }

    public MaxSizePolicy getMaxSizePolicy() {
        return maxSizePolicy;
    }

    public MaxSizeConfig setMaxSizePolicy(MaxSizePolicy maxSizePolicy) {
        this.maxSizePolicy = maxSizePolicy;
        return this;
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
