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

import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;

import java.util.HashSet;
import java.util.Set;

public class QueryService {

    final IndexService indexService = new IndexService();

    public Set<QueryableEntry> query(Predicate predicate, Set<QueryableEntry> allEntries) {
        QueryContext queryContext = new QueryContext(indexService);
        Set<QueryableEntry> result = null;
        if (predicate instanceof IndexAwarePredicate) {
            IndexAwarePredicate iap = (IndexAwarePredicate) predicate;
            if (iap.isIndexed(queryContext)) {
                result = iap.filter(queryContext);
            }
        }
        if (result == null) {
            result = new HashSet<QueryableEntry>();
            for (QueryableEntry entry : allEntries) {
                if (predicate.apply(entry)) {
                    result.add(entry);
                }
            }
        }
        return result;
    }
}
