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

package com.hazelcast.map;

import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.Map;
import java.util.Set;

public class TestPredicate implements IndexAwarePredicate {

    private String value;
    private boolean didApply;

    public TestPredicate(String value) {
        this.value = value;
    }
    
    public boolean apply(Map.Entry mapEntry) {
        didApply = true;
        TempData data = (TempData) mapEntry.getValue();
        return data.getAttr1().equals(value);
    }

    @Override
    public boolean in(Predicate predicate) {
        return false;
    }

    public Set<QueryableEntry> filter(QueryContext queryContext) {
        return queryContext.getIndex("attr1").getRecords(value);
    }

    public boolean isIndexed(QueryContext queryContext) {
        return queryContext.getIndex("attr1") != null;
    }

    public boolean didApply() {
        return didApply;
    }

}