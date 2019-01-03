/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.eviction;

import com.hazelcast.config.ConfigDataSerializerHook;
import com.hazelcast.core.EntryView;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * LFU eviction policy for an {@link com.hazelcast.core.IMap IMap}
 */
public class LFUEvictionPolicy extends MapEvictionPolicy implements IdentifiedDataSerializable {

    /**
     * LFU eviction policy instance.
     */
    public static final LFUEvictionPolicy INSTANCE = new LFUEvictionPolicy();

    @Override
    public int compare(EntryView entryView1, EntryView entryView2) {
        long hits1 = entryView1.getHits();
        long hits2 = entryView2.getHits();
        return (hits1 < hits2) ? -1 : ((hits1 == hits2) ? 0 : 1);
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.LFU_EVICTION_POLICY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        //no-op
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        //no-op
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        return this.getClass().equals(obj.getClass());
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode();
    }
}
