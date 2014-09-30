/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.Data;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * This interface stores indexes of Query.
 */
public interface IndexStore {

    void newIndex(Comparable newValue, QueryableEntry entry);
    void updateIndex(Comparable oldValue, Comparable newValue, QueryableEntry entry);
    void removeIndex(Comparable oldValue, Data indexKey);
    void clear();

    void getSubRecordsBetween(MultiResultSet results, Comparable from, Comparable to);
    void getSubRecords(MultiResultSet results, ComparisonType comparisonType, Comparable searchedValue);
    Set<QueryableEntry> getRecords(Comparable value);
    void getRecords(MultiResultSet results, Set<Comparable> values);
    ConcurrentMap<Data, QueryableEntry> getRecordMap(Comparable indexValue);
}
