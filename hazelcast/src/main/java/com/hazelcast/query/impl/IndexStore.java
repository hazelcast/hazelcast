/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.monitor.impl.IndexOperationStats;
import com.hazelcast.nio.serialization.Data;

import java.util.Set;

/**
 * This interface stores indexes of Query.
 */
public interface IndexStore {

    void newIndex(Object newValue, QueryableEntry entry, IndexOperationStats operationStats);
    void updateIndex(Object oldValue, Object newValue, QueryableEntry entry, IndexOperationStats operationStats);
    void removeIndex(Object oldValue, Data indexKey, IndexOperationStats operationStats);
    void clear();
    void destroy();

    Set<QueryableEntry> getSubRecordsBetween(Comparable from, Comparable to);
    Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedValue);
    Set<QueryableEntry> getRecords(Comparable value);
    Set<QueryableEntry> getRecords(Set<Comparable> values);
}
