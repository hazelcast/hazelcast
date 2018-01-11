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

package com.hazelcast.replicatedmap.merge;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.operation.ReplicatedMapDataSerializerHook;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView;

/**
 * Merges replicated map entries from source to destination directly unless the merging entry is {@code null}.
 */
public final class PassThroughMergePolicy implements ReplicatedMapMergePolicy, IdentifiedDataSerializable {

    /**
     * Single instance of this class.
     */
    public static final PassThroughMergePolicy INSTANCE = new PassThroughMergePolicy();

    private PassThroughMergePolicy() {
    }

    @Override
    public Object merge(String mapName, ReplicatedMapEntryView mergingEntry, ReplicatedMapEntryView existingEntry) {
        return mergingEntry == null ? existingEntry.getValue() : mergingEntry.getValue();
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.PASS_THROUGH_MERGE_POLICY;
    }

    @Override
    public void writeData(ObjectDataOutput out) {
    }

    @Override
    public void readData(ObjectDataInput in) {
    }
}
