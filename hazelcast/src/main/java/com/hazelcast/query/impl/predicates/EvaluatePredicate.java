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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Set;

/**
 * Wraps a predicate for which {@link Index#canEvaluate} returned {@code true}.
 * <p>
 * Used during predicate tree optimization done by {@link EvaluateVisitor}.
 * Never transferred over the wire.
 */
public final class EvaluatePredicate implements Predicate, IndexAwarePredicate {

    private final Predicate predicate;
    private final String indexName;

    /**
     * Constructs {@link EvaluatePredicate} instance for the given predicate
     * which can be evaluated by the given index identified by its name.
     *
     * @param predicate the evaluable predicate to wrap.
     * @param indexName the index which can evaluate the given predicate.
     */
    public EvaluatePredicate(Predicate predicate, String indexName) {
        this.predicate = predicate;
        this.indexName = indexName;
    }

    /**
     * @return the original evaluable predicate.
     */
    public Predicate getPredicate() {
        return predicate;
    }

    /**
     * @return the index name of the index which can evaluate the predicate
     * wrapped by this predicate instance.
     */
    public String getIndexName() {
        return indexName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean apply(Map.Entry mapEntry) {
        return predicate.apply(mapEntry);
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Index index = queryContext.matchIndex(indexName, QueryContext.IndexMatchHint.EXACT_NAME);
        if (index == null) {
            return null;
        }
        return index.evaluate(predicate);
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        return true;
    }

    @Override
    public String toString() {
        return "eval(" + predicate.toString() + ")";
    }

    private void writeObject(ObjectOutputStream stream) throws IOException {
        throw new UnsupportedOperationException("can't be serialized");
    }

}
