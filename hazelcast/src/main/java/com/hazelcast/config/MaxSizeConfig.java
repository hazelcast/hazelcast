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

public class MaxSizeConfig implements DataSerializable {
    int size = MapConfig.DEFAULT_MAX_SIZE;
    MaxSizePolicy maxSizePolicy = MaxSizePolicy.PER_NODE;

    public enum MaxSizePolicy {
        PER_NODE, PER_PARTITION, USED_HEAP_PERCENTAGE, USED_HEAP_SIZE
    }

    public void readData(ObjectDataInput in) throws IOException {
        size = in.readInt();
        maxSizePolicy = MaxSizePolicy.valueOf(in.readUTF());
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(size);
        out.writeUTF(maxSizePolicy.name());
    }

    public int getSize() {
        return size;
    }

    public MaxSizeConfig setSize(int size) {
        if (size <= 0) {
            size = Integer.MAX_VALUE;
        }
        this.size = size;
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
        return "MaxSizeConfig{" +
                "maxSizePolicy='" + maxSizePolicy + '\'' +
                ", size=" + size +
                '}';
    }
}
