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

package com.hazelcast.map.merge;

import com.hazelcast.core.EntryView;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import java.io.IOException;


/**
 * Built-in MergePolicy implementation.
 * <p/>
 * HigherHitsMapMergePolicy causes the merging entry to be merged from source to destination map
 * if source entry has higher hits than the destination one.
 * <p/>
 * <p/>
 *
 * @see com.hazelcast.map.merge.MapMergePolicy
 * @see com.hazelcast.map.merge.PutIfAbsentMapMergePolicy
 * @see com.hazelcast.map.merge.LatestUpdateMapMergePolicy
 * @see com.hazelcast.map.merge.PassThroughMergePolicy
 */
public class HigherHitsMapMergePolicy implements MapMergePolicy, DataSerializable {

    public Object merge(String mapName, EntryView mergingEntry, EntryView existingEntry) {
        if (mergingEntry.getHits() >= existingEntry.getHits()) {
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
}
