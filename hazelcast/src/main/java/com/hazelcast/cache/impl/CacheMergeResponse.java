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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;
import com.hazelcast.wan.impl.CallerProvenance;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Special response class used to provide verbose results for {@link com.hazelcast.cache.ICache}
 * merge operations, specifically from {@link ICacheRecordStore#merge(SplitBrainMergeTypes.CacheMergeTypes,
 * SplitBrainMergePolicy, CallerProvenance)}
 */
public class CacheMergeResponse {
    @Nullable
    private final CacheRecord record;
    @Nonnull
    private final MergeResult result;

    public CacheMergeResponse(@Nullable CacheRecord record, @Nonnull MergeResult result) {
        this.record = record;
        this.result = result;
    }

    @Nullable
    public CacheRecord getRecord() {
        return record;
    }

    @Nonnull
    public MergeResult getResult() {
        return result;
    }

    public enum MergeResult {
        NO_MERGE_APPLIED(false),
        RECORDS_ARE_EQUAL(true),
        RECORD_EXPIRY_UPDATED(true),
        RECORD_CREATED(true),
        RECORD_UPDATED(true),
        ;

        private final boolean mergeApplied;

        MergeResult(boolean mergeApplied) {
            this.mergeApplied = mergeApplied;
        }

        public boolean isMergeApplied() {
            return mergeApplied;
        }
    }
}
