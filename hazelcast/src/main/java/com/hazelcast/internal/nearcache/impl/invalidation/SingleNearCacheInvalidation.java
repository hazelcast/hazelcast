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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.map.impl.MapDataSerializerHook.NEAR_CACHE_SINGLE_INVALIDATION;

/**
 * Represents a single key invalidation.
 */
public class SingleNearCacheInvalidation extends Invalidation {

    private Data key;

    public SingleNearCacheInvalidation() {
    }

    public SingleNearCacheInvalidation(Data key, String dataStructureName, UUID sourceUuid,
                                       UUID partitionUuid, long sequence) {
        super(dataStructureName, sourceUuid, partitionUuid, sequence);
        // key can be null when null it means this event is a clear event.
        this.key = key;
    }

    @Override
    public final Data getKey() {
        return key;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        IOUtil.writeData(out, key);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        key = IOUtil.readData(in);
    }

    @Override
    public String toString() {
        return "SingleNearCacheInvalidation{"
                + "key=" + key
                + ", " + super.toString()
                + '}';
    }

    @Override
    public int getClassId() {
        return NEAR_CACHE_SINGLE_INVALIDATION;
    }
}
