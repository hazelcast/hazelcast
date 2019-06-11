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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.core.TypeConverter;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.Map;
import java.util.Set;

public class EvaluatePredicate implements Predicate, IndexAwarePredicate {

    private final Predicate predicate;
    private final String indexName;
    private final TypeConverter converter;

    public EvaluatePredicate(Predicate predicate, String indexName, TypeConverter converter) {
        this.predicate = predicate;
        this.indexName = indexName;
        this.converter = converter;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public String getIndexName() {
        return indexName;
    }

    public TypeConverter getConverter() {
        return converter;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean apply(Map.Entry mapEntry) {
        return predicate.apply(mapEntry);
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        return queryContext.matchIndex(indexName, QueryContext.IndexMatchHint.EXACT_NAME).evaluate(predicate, converter);
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        return true;
    }

}
