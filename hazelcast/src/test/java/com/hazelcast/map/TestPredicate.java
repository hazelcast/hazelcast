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

package com.hazelcast.map;

import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.Map;
import java.util.Set;

public class TestPredicate implements IndexAwarePredicate<String, TempData> {

    private String value;

    public TestPredicate(String value) {
        this.value = value;
    }

    public boolean apply(Map.Entry<String, TempData> mapEntry) {
        TestPredicateState.counter++;
        TempData data = (TempData) mapEntry.getValue();
        return data.getAttr1().equals(value);
    }

    public Set<QueryableEntry<String, TempData>> filter(QueryContext queryContext) {
        TestPredicateState.filtered = true;
        return (Set) queryContext.getIndex("attr1").getRecords(value);
    }

    public boolean isIndexed(QueryContext queryContext) {
        return queryContext.getIndex("attr1") != null;
    }

    public boolean isFilteredAndAppliedOnlyOnce() {
        return TestPredicateState.filtered && TestPredicateState.counter == 1;
    }

    public int getApplied() {
        return TestPredicateState.counter;
    }

    // predicates are cloned for thread-safety thus their state is not modified
    // for test purposes, we store the state in the static context, since the predicate is used once
    private static class TestPredicateState {
        public static int counter = 0;
        public static boolean filtered = false;
    }

}