/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.TypeConverter;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.QueryException;

import java.util.Set;

/**
 * This interface contains the methods related to index of Query.
 */
public interface Index {

    void clear();

    /**
     * Add entry to this index.
     * @param e entry
     * @param oldValue or null if there is no old value
     * @throws QueryException
     */
    void saveEntryIndex(QueryableEntry e, Object oldValue) throws QueryException;

    /**
     * Return converter associated with this Index.
     * It can return <code>null</code> if no entry has been saved yet.
     *
     * @return
     */
    TypeConverter getConverter();

    void removeEntryIndex(Data key, Object value);

    Set<QueryableEntry> getRecords(Comparable[] values);

    Set<QueryableEntry> getRecords(Comparable value);

    Set<QueryableEntry> getSubRecordsBetween(Comparable from, Comparable to);

    Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedValue);

    String getAttributeName();

    boolean isOrdered();
}
