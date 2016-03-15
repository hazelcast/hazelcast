/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.monitor;

/**
 * Wrapper interface for LocalMapStats. This interface
 * is used in {@link com.hazelcast.map.impl.proxy.MapProxySupport}
 * which is where the stats are updated. If statistics are not
 * enabled, Null implementation is used.
 */
public interface MapStatsCounter {

    /**
     * Updates hits, lastAccessTime but not getOperationCount
     * @param startTime operation time.
     */
    void updateContainsStats(long startTime);

    /**
     * Updates getOperationCount. Updates hits and
     * lastAccessTime if {@param result} is not null.
     * @param startTime operation time.
     * @param result result of the get operation
     */
    void updateGetStats(long startTime, Object result);

    /**
     * Updates hit count and lastAccessTime
     * caused by getAll operation.
     * @param startTime operation time.
     * @param hits number of hits.
     */
    void updateGetStats(long startTime, long hits);

    /**
     * Updates putOperationCount and lastUpdateTime
     * caused by put operation.
     * @param startTime operation time.
     */
    void updatePutStats(long startTime);

    /**
     * Updates putOperationCount and lastUpdateTime
     * caused by putAll operation.
     * @param startTime operation time.
     * @param putCount number of puts.
     */
    void updatePutStats(long startTime, long putCount);

    /**
     * Updates lastUpdateTime caused by entry processor actions
     * @param startTime operation time.
     */
    void updateEntryProcessorStats(long startTime);

    /**
     * Updates removeOperationCount.
     * @param startTime operation time.
     */
    void updateRemoveStats(long startTime);

    /**
     * Updates putOperationCount and lastUpdateTime if {@param result}
     * is null. Updates hits, lastAccessTime if {@param result} not null.
     * @param startTime operation time.
     * @param result result of putIfAbsentOperation
     */
    void updatePutIfAbsentStats(long startTime, Object result);
}
