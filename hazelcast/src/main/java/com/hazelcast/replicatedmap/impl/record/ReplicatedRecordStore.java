/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.ReplicatedMapMergeTypes;
import com.hazelcast.internal.util.scheduler.ScheduledEntry;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This interface describes a common record store for replicated maps and their actual records
 */
@SuppressWarnings("checkstyle:methodcount")
public interface ReplicatedRecordStore {

    String getName();

    int getPartitionId();

    Object remove(Object key);

    Object removeWithVersion(Object key, long version);

    void evict(Object key);

    Object get(Object key);

    Object put(Object key, Object value);

    Object put(Object key, Object value, long ttl, TimeUnit timeUnit, boolean incrementHits);

    Object putWithVersion(Object key, Object value, long ttl, TimeUnit timeUnit, boolean incrementHits, long version);

    boolean containsKey(Object key);

    boolean containsValue(Object value);

    ReplicatedRecord getReplicatedRecord(Object key);

    Set keySet(boolean lazy);

    Collection values(boolean lazy);

    Collection values(Comparator comparator);

    Set entrySet(boolean lazy);

    int size();

    void clear();

    void clearWithVersion(long version);

    void reset();

    boolean isEmpty();

    Object unmarshall(Object key);

    Object marshall(Object key);

    void destroy();

    long getVersion();

    boolean isStale(long version);

    Iterator<ReplicatedRecord> recordIterator();

    void putRecords(Collection<RecordMigrationInfo> records, long version);

    InternalReplicatedMapStorage getStorage();

    ScheduledEntry<Object, Object> cancelTtlEntry(Object key);

    boolean scheduleTtlEntry(long delayMillis, Object key, Object object);

    boolean isLoaded();

    void setLoaded(boolean loaded);

    /**
     * Merges the given {@link ReplicatedMapMergeTypes} via the given {@link SplitBrainMergePolicy}.
     *
     * @param mergingEntry the {@link ReplicatedMapMergeTypes} instance to merge
     * @param mergePolicy  the {@link SplitBrainMergePolicy} instance to apply
     * @return {@code true} if merge is applied, otherwise {@code false}
     */
    boolean merge(ReplicatedMapMergeTypes<Object, Object> mergingEntry,
                  SplitBrainMergePolicy<Object, ReplicatedMapMergeTypes<Object, Object>, Object> mergePolicy);
}
