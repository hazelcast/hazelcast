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

package com.hazelcast.map;

import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.Map;
import java.util.Set;

class TestPredicate implements IndexAwarePredicate<String, TestData> {

    private String value;
    private boolean filtered;
    private int applied;

    TestPredicate(String value) {
        this.value = value;
    }

    @Override
    public boolean apply(Map.Entry<String, TestData> mapEntry) {
        applied++;
        TestData data = mapEntry.getValue();
        return data.getAttr1().equals(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<QueryableEntry<String, TestData>> filter(QueryContext queryContext) {
        filtered = true;
        return (Set) queryContext.getIndex("attr1").getRecords(value);
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        return queryContext.getIndex("attr1") != null;
    }

    boolean isFilteredAndApplied(int times) {
        return filtered && applied == times;
    }

    int getApplied() {
        return applied;
    }
}
