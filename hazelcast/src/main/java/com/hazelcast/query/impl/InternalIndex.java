/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.monitor.impl.PerIndexStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Index;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.QueryableEntry;

import java.util.Set;

/**
 * Provides the private index API.
 */
public interface InternalIndex extends Index {

    /**
     * Saves the given entry into this index.
     *
     * @param entry           the entry to save.
     * @param oldValue        the previous old value associated with the entry or
     *                        {@code null} if the entry is new.
     * @param operationSource the operation source.
     * @throws QueryException if there were errors while extracting the
     *                        attribute value from the entry.
     */
    void putEntry(QueryableEntryImpl entry, Object oldValue, OperationSource operationSource);

    /**
     * Removes the entry having the given key and the value from this index.
     *
     * @param key             the key of the entry to remove.
     * @param value           the value of the entry to remove.
     * @param operationSource the operation source.
     * @throws QueryException if there were errors while extracting the
     *                        attribute value from the entry.
     */
    void removeEntry(Data key, Object value, OperationSource operationSource);

    /**
     * Produces a result set containing entries whose attribute values are
     * satisfy the comparison of the given type with the given value.
     *
     * @param comparison the type of the comparison to perform.
     * @param value      the value to compare against.
     * @return the produced result set.
     */
    Set<? extends QueryableEntry> getRecords(Comparison comparison, Comparable value);

    /**
     * Clears out all entries from this index.
     */
    void clear();

    /**
     * Releases all resources hold by this index, e.g. the allocated native
     * memory for the HD index.
     */
    void destroy();

    /**
     * Identifies an original source of an index operation.
     * <p>
     * Required for the index stats tracking to ignore index operations
     * initiated internally by Hazelcast. We can't achieve the same behaviour
     * on the pure stats level, e.g. by turning stats off during a partition
     * migration, since global indexes and their stats are shared across
     * partitions.
     */
    enum OperationSource {

        /**
         * Indicates that an index operation was initiated by a user; for
         * instance, as a result of a new map entry insertion.
         */
        USER,

        /**
         * Indicates that an index operation was initiated internally by
         * Hazelcast; for instance, as a result of a partition migration.
         */
        SYSTEM

    }
    /**
     * Canonicalizes the given value for the purpose of a hash-based lookup.
     * <p>
     * The method is used while performing InPredicate queries to canonicalize
     * the set of values in question, so additional duplicate-eliminating
     * post-processing step can be avoided.
     *
     * @param value the value to canonicalize.
     * @return the canonicalized value.
     */
    Comparable canonicalizeQueryArgumentScalar(Comparable value);

    /**
     * Returns {@code true} if the given partition is indexed by this index,
     * {@code false} otherwise.
     */
    boolean hasPartitionIndexed(int partitionId);

    /**
     * Marks the given partition as indexed by this index.
     *
     * @param partitionId the ID of the partition to mark as indexed.
     */
    void markPartitionAsIndexed(int partitionId);

    /**
     * Marks the given partition as unindexed by this index.
     *
     * @param partitionId the ID of the partition to mark as unindexed.
     */
    void markPartitionAsUnindexed(int partitionId);

    /**
     * Returns the index stats associated with this index.
     */
    PerIndexStats getPerIndexStats();

}
