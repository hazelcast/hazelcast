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

package com.hazelcast.replicatedmap.merge;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.operation.ReplicatedMapDataSerializerHook;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView;

import java.io.IOException;

/**
 * LatestUpdateMapMergePolicy causes the merging entry to be merged from source to destination map
 * if source entry has updated more recently than the destination entry.
 * <p/>
 * This policy can only be used of the clocks of the machines are in sync.
 */
public final class LatestUpdateMapMergePolicy implements ReplicatedMapMergePolicy, IdentifiedDataSerializable {

    /**
     * Single instance of this class
     */
    public static final LatestUpdateMapMergePolicy INSTANCE = new LatestUpdateMapMergePolicy();

    private LatestUpdateMapMergePolicy() {
    }

    @Override
    public Object merge(String mapName, ReplicatedMapEntryView mergingEntry, ReplicatedMapEntryView existingEntry) {
        if (mergingEntry.getLastUpdateTime() >= existingEntry.getLastUpdateTime()) {
            return mergingEntry.getValue();
        }
        return existingEntry.getValue();
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.LATEST_UPDATE_MERGE_POLICY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }

    private Object readResolve() {
        return INSTANCE;
    }
}
