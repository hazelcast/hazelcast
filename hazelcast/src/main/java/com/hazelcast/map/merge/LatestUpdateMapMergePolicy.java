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

package com.hazelcast.map.merge;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Merges map entries from source to destination map if the source entry
 * was updated more recently than the destination entry.
 * <p>
 * <b>Note:</b> This policy can only be used if the clocks of the nodes are in sync.
 */
public class LatestUpdateMapMergePolicy implements MapMergePolicy, IdentifiedDataSerializable {

    @Override
    public Object merge(String mapName, EntryView mergingEntry, EntryView existingEntry) {
        if (mergingEntry.getLastUpdateTime() >= existingEntry.getLastUpdateTime()) {
            return mergingEntry.getValue();
        }
        return existingEntry.getValue();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.LATEST_UPDATE_MERGE_POLICY;
    }
}
