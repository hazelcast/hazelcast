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

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MaxSizeConfig implements DataSerializable {
    public static final String POLICY_MAP_SIZE_PER_JVM = "map_size_per_jvm";
    public static final String POLICY_CLUSTER_WIDE_MAP_SIZE = "cluster_wide_map_size";
    public static final String POLICY_PARTITIONS_WIDE_MAP_SIZE = "partitions_wide_map_size";
    public static final String POLICY_USED_HEAP_SIZE = "used_heap_size";
    public static final String POLICY_USED_HEAP_PERCENTAGE = "used_heap_percentage";
    int size = MapConfig.DEFAULT_MAX_SIZE;
    String maxSizePolicy = POLICY_CLUSTER_WIDE_MAP_SIZE;

    public void readData(DataInput in) throws IOException {
        size = in.readInt();
        maxSizePolicy = in.readUTF();
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(size);
        out.writeUTF(maxSizePolicy);
    }

    public int getSize() {
        return size;
    }

    public MaxSizeConfig setSize(int size) {
        if (size == 0) {
            size = Integer.MAX_VALUE;
        }
        this.size = size;
        return this;
    }

    public String getMaxSizePolicy() {
        return maxSizePolicy;
    }

    public MaxSizeConfig setMaxSizePolicy(String maxSizePolicy) {
        this.maxSizePolicy = maxSizePolicy;
        return this;
    }

    @Override
    public String toString() {
        return "MaxSizeConfig{" +
                "maxSizePolicy='" + maxSizePolicy + '\'' +
                ", size=" + size +
                '}';
    }
}
