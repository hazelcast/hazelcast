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

/**
 * Statistics on collection level that are collected on demand.
 * Some of them are aggregated separately for owned
 * and backup entries (without distinguishing replica index).
 */
public interface CollectionOnDemandStats {
    OnDemandStats owned();

    OnDemandStats backup();

    /**
     * Estimated number of bytes used on the JVM heap by the collection.
     * Includes also memory that cannot be attributed to owned or backup partitions.
     */
    long heapBytesUsed();

    VectorIndexStats getVectorIndexStats();
}
