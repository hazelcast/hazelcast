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

package com.hazelcast.query.impl;

/**
 * Provides the context for queries execution.
 */
public class QueryContext {

    protected Indexes indexes;
    protected int ownedPartitionCount = -1;

    /**
     * Creates a new query context with the given available indexes.
     *
     * @param indexes the indexes available for the query context.
     */
    public QueryContext(Indexes indexes, int ownedPartitionCount) {
        this.indexes = indexes;
        this.ownedPartitionCount = ownedPartitionCount;
    }

    /**
     * Creates a new query context unattached to any indexes.
     */
    QueryContext() {
    }

    /**
     * @return a count of owned partitions a query runs on.
     */
    public int getOwnedPartitionCount() {
        return ownedPartitionCount;
    }

    /**
     * Sets owned partitions count a query runs on.
     * @param ownedPartitionCount a count of owned partitions.
     */
    public void setOwnedPartitionCount(int ownedPartitionCount) {
        this.ownedPartitionCount = ownedPartitionCount;
    }

    /**
     * Attaches this index context to the given indexes.
     *
     * @param indexes the indexes to attach to.
     * @param ownedPartitionCount a count of owned partitions a query runs on.
     *                            Negative value indicates that the value is not defined.
     *
     */
    void attachTo(Indexes indexes, int ownedPartitionCount) {
        this.indexes = indexes;
        this.ownedPartitionCount = ownedPartitionCount;
    }

    /**
     * Applies the collected per-query stats, if any.
     */
    void applyPerQueryStats() {
        // do nothing
    }

    /**
     * Obtains the index available for the given attribute in this query
     * context.
     *
     * @param attribute the attribute to obtain the index for.
     * @return the obtained index or {@code null} if there is no index available
     * for the given attribute.
     */
    public Index getIndex(String attribute) {
        return matchIndex(attribute, IndexMatchHint.NONE);
    }

    /**
     * Matches an index for the given pattern and match hint.
     *
     * @param pattern   the pattern to match an index for. May be either an
     *                  attribute name or an exact index name.
     * @param matchHint the match hint.
     * @return the matched index or {@code null} if nothing matched.
     * @see QueryContext.IndexMatchHint
     */
    public Index matchIndex(String pattern, IndexMatchHint matchHint) {
        return indexes.matchIndex(pattern, matchHint, ownedPartitionCount);
    }

    /**
     * Defines possible index matching hints.
     */
    public enum IndexMatchHint {

        /**
         * Match pattern is interpreted as an attribute name. An index is
         * matched without any preferences of ordered indexes over unordered
         * ones and vice versa.
         */
        NONE,

        /**
         * Match pattern is interpreted as an attribute name. An index is
         * matched with a preference of unordered indexes over ordered ones.
         */
        PREFER_UNORDERED,

        /**
         * Match pattern interpreted is as an attribute name. An index is
         * matched with a preference of ordered indexes over unordered ones.
         */
        PREFER_ORDERED,

        /**
         * Match pattern is interpreted as a full index name and may specify a
         * composite index name.
         */
        EXACT_NAME

    }

}
