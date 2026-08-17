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

package com.hazelcast.vector.internal.impl.stats;

import java.util.Objects;

public final class OnDemandStatsImpl implements OnDemandStats {
    private long size;
    private long heapBytesUsed;

    public OnDemandStatsImpl(long size, long heapBytesUsed) {
        this.size = size;
        this.heapBytesUsed = heapBytesUsed;
    }

    public OnDemandStatsImpl() {
        this(0, 0);
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long heapBytesUsed() {
        return heapBytesUsed;
    }

    /**
     * Adds other stats to this object in place
     *
     * @return this
     */
    public OnDemandStatsImpl add(OnDemandStats other) {
        size += other.size();
        heapBytesUsed += other.heapBytesUsed();
        return this;
    }

    public OnDemandStatsImpl addHeapBytes(long heapBytesUsed) {
        this.heapBytesUsed += heapBytesUsed;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (OnDemandStatsImpl) obj;
        return this.size == that.size
                && this.heapBytesUsed == that.heapBytesUsed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(size, heapBytesUsed);
    }

    @Override
    public String toString() {
        return "OnDemandStats["
                + "size=" + size + ", "
                + "heapBytesUsed=" + heapBytesUsed + ']';
    }
}
