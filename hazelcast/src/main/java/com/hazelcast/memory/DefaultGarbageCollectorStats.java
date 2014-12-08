/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.memory;

/**
 * Default implementation of GarbageCollectorStats.
 */
public class DefaultGarbageCollectorStats implements GarbageCollectorStats {

    private volatile long minorCount;
    private volatile long minorTime;
    private volatile long majorCount;
    private volatile long majorTime;
    private volatile long unknownCount;
    private volatile long unknownTime;

    @Override
    public long getMajorCollectionCount() {
        return majorCount;
    }

    @Override
    public long getMajorCollectionTime() {
        return majorTime;
    }

    @Override
    public long getMinorCollectionCount() {
        return minorCount;
    }

    @Override
    public long getMinorCollectionTime() {
        return minorTime;
    }

    @Override
    public long getUnknownCollectionCount() {
        return unknownCount;
    }

    @Override
    public long getUnknownCollectionTime() {
        return unknownTime;
    }

    void setMinorCount(long minorCount) {
        this.minorCount = minorCount;
    }

    void setMinorTime(long minorTime) {
        this.minorTime = minorTime;
    }

    void setMajorCount(long majorCount) {
        this.majorCount = majorCount;
    }

    void setMajorTime(long majorTime) {
        this.majorTime = majorTime;
    }

    void setUnknownCount(long unknownCount) {
        this.unknownCount = unknownCount;
    }

    void setUnknownTime(long unknownTime) {
        this.unknownTime = unknownTime;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("GarbageCollectorStats {");
        sb.append("MinorGC -> Count: ").append(minorCount).append(", Time (ms): ").append(minorTime)
                .append(", MajorGC -> Count: ").append(majorCount).append(", Time (ms): ").append(majorTime);
        if (unknownCount > 0) {
            sb.append(", UnknownGC -> Count: ").append(unknownCount)
                    .append(", Time (ms): ").append(unknownTime);
        }
        sb.append('}');
        return sb.toString();
    }
}
