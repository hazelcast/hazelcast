/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.stats;

import java.util.Objects;

public final class CollectionOnDemandStatsImpl implements CollectionOnDemandStats {
    private static final OnDemandStatsImpl ZERO_STATS = new OnDemandStatsImpl();
    private static final VectorIndexStats ZERO_INDEX_STATS = new VectorIndexStatsImpl();
    private final OnDemandStatsImpl owned;
    private final OnDemandStatsImpl backup;
    private final long heapBytesUsed;
    private final VectorIndexStats vectorIndexStats;

    public CollectionOnDemandStatsImpl(OnDemandStatsImpl owned, OnDemandStatsImpl backup, long heapBytesUsed,
                                       VectorIndexStats vectorIndexStats) {
        this.owned = owned;
        this.backup = backup;
        this.heapBytesUsed = heapBytesUsed;
        this.vectorIndexStats = vectorIndexStats;
    }

    public CollectionOnDemandStatsImpl(long heapBytesUsed) {
        this(ZERO_STATS, ZERO_STATS, heapBytesUsed, ZERO_INDEX_STATS);
    }

    @Override
    public OnDemandStats owned() {
        return owned;
    }

    @Override
    public OnDemandStats backup() {
        return backup;
    }

    @Override
    public long heapBytesUsed() {
        return heapBytesUsed;
    }

    @Override
    public VectorIndexStats getVectorIndexStats() {
        return vectorIndexStats;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (CollectionOnDemandStatsImpl) obj;
        return Objects.equals(this.owned, that.owned)
                && Objects.equals(this.backup, that.backup)
                && this.heapBytesUsed == that.heapBytesUsed
                && Objects.equals(this.vectorIndexStats, that.vectorIndexStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(owned, backup, heapBytesUsed, vectorIndexStats);
    }

    @Override
    public String toString() {
        return "CollectionOnDemandStats["
                + "owned=" + owned + ", "
                + "backup=" + backup + ", "
                + "heapBytesUsed=" + heapBytesUsed + ", "
                + "vectorIndexStats=" + vectorIndexStats + ']';
    }
}
