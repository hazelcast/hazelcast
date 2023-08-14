/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;
import com.hazelcast.wan.impl.CallerProvenance;

/**
 * Special response class used to provide verbose results for {@link com.hazelcast.map.IMap}
 * merge operations, specifically from {@link RecordStore#merge(SplitBrainMergeTypes.MapMergeTypes,
 * SplitBrainMergePolicy, CallerProvenance)}
 */
public enum MapMergeResponse {
    NO_MERGE_APPLIED(false),
    RECORDS_ARE_EQUAL(true),
    RECORD_EXPIRY_UPDATED(true),
    RECORD_REMOVED(true),
    RECORD_CREATED(true),
    RECORD_UPDATED(true),
    ;

    private final boolean mergeApplied;

    MapMergeResponse(boolean mergeApplied) {
        this.mergeApplied = mergeApplied;
    }

    public boolean isMergeApplied() {
        return mergeApplied;
    }
}
