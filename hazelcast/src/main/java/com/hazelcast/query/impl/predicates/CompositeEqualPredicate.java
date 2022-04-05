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
import com.hazelcast.query.impl.CompositeValue;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * Does the same thing as {@link EqualPredicate} but for composite indexes and
 * values.
 */
@SuppressFBWarnings("SE_BAD_FIELD")
public class CompositeEqualPredicate implements IndexAwarePredicate {

    final String indexName;
    final String[] components;
    final CompositeValue value;

    private volatile Predicate fallbackPredicate;

    /**
     * Constructs a new composite equal predicate for the given index and
     * composite value.
     *
     * @param index the index to construct the predicate on.
     * @param value the value to construct the predicate for.
     */
    public CompositeEqualPredicate(InternalIndex index, CompositeValue value) {
        // We can't store a direct index reference here, the actual index must
        // always be obtained from the QueryContext while executing the query to
        // make index stats work properly.
        this.indexName = index.getName();
        this.components = index.getComponents();
        this.value = value;
    }

    // for testing purposes
    CompositeEqualPredicate(String indexName, String[] components, CompositeValue value) {
        this.indexName = indexName;
        this.components = components;
        this.value = value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean apply(Map.Entry entry) {
        // Predicates may still be asked to downgrade to no-index execution even
        // if there are suitable indexes available. For instance, that may
        // happen during migrations.

        if (fallbackPredicate == null) {
            generateFallbackPredicate();
        }
        return fallbackPredicate.apply(entry);
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Index index = queryContext.matchIndex(indexName, QueryContext.IndexMatchHint.EXACT_NAME);
        if (index == null) {
            return null;
        }
        return index.getRecords(value);
    }

    @Override
    public String toString() {
        return Arrays.toString(components) + " = " + value;
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        return true;
    }

    private void generateFallbackPredicate() {
        // here we are reversing the work of CompositeIndexVisitor.generateEqualPredicate

        Comparable[] values = value.getComponents();

        Predicate[] predicates = new Predicate[components.length];
        for (int i = 0; i < components.length; ++i) {
            predicates[i] = new EqualPredicate(components[i], values[i]);
        }
        fallbackPredicate = new AndPredicate(predicates);
    }

    private void writeObject(ObjectOutputStream stream) throws IOException {
        throw new UnsupportedOperationException("can't be serialized");
    }

}
