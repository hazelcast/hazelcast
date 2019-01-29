/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.TypedDataSerializable;
import com.hazelcast.nio.serialization.BinaryInterface;

import java.io.IOException;

/**
 * Configuration for cache eviction (used for backward compatibility).
 *
 * Comparator support is not provided for compatibility.
 */
@BinaryInterface
public class LegacyCacheEvictionConfig implements TypedDataSerializable {

    final CacheEvictionConfig config;

    @SuppressWarnings("unused")
    public LegacyCacheEvictionConfig() {
        config = new CacheEvictionConfig();
    }

    public LegacyCacheEvictionConfig(CacheEvictionConfig evictionConfig) {
        config = evictionConfig;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(config.getSize());
        out.writeUTF(config.getMaxSizePolicy().toString());
        out.writeUTF(config.getEvictionPolicy().toString());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        config.setSize(in.readInt());
        config.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.valueOf(in.readUTF()));
        config.setEvictionPolicy(EvictionPolicy.valueOf(in.readUTF()));
    }

    @Override
    public Class getClassType() {
        return CacheEvictionConfig.class;
    }

    public CacheEvictionConfig getConfig() {
        return config;
    }
}
