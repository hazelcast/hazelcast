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

import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static com.hazelcast.query.impl.CompositeValue.NEGATIVE_INFINITY;
import static com.hazelcast.query.impl.CompositeValue.POSITIVE_INFINITY;

/**
 * Does the same thing as {@link GreaterLessPredicate}, {@link BetweenPredicate}
 * and {@link BoundedRangePredicate} but for composite indexes and values.
 */
@SuppressFBWarnings("SE_BAD_FIELD")
public class CompositeRangePredicate implements IndexAwarePredicate {

    final String indexName;
    final String[] components;

    final CompositeValue from;
    final boolean fromInclusive;

    final CompositeValue to;
    final boolean toInclusive;

    private final int prefixLength;

    private volatile Predicate fallbackPredicate;

    /**
     * Constructs a new composite range predicate on the given index.
     *
     * @param index         the index to construct the predicate on.
     * @param from          the lower/left range bound.
     * @param fromInclusive {@code true} if the range is left-closed,
     *                      {@code false} otherwise.
     * @param to            the upper/right range bound.
     * @param toInclusive   {@code true} if the range is right-closed,
     *                      {@code false} otherwise.
     */
    public CompositeRangePredicate(InternalIndex index, CompositeValue from, boolean fromInclusive, CompositeValue to,
                                   boolean toInclusive, int prefixLength) {
        if (from == null || to == null) {
            throw new IllegalArgumentException("range must be bounded");
        }

        // We can't store a direct index reference here, the actual index must
        // always be obtained from the QueryContext while executing the query to
        // make index stats work properly.
        this.indexName = index.getName();
        this.components = index.getComponents();

        this.from = from;
        this.fromInclusive = fromInclusive;

        this.to = to;
        this.toInclusive = toInclusive;

        this.prefixLength = prefixLength;
    }

    // for testing purposes
    CompositeRangePredicate(String indexName, String[] components, CompositeValue from, boolean fromInclusive, CompositeValue to,
                            boolean toInclusive, int prefixLength) {
        this.indexName = indexName;
        this.components = components;

        this.from = from;
        this.fromInclusive = fromInclusive;

        this.to = to;
        this.toInclusive = toInclusive;

        this.prefixLength = prefixLength;
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
        return index.getRecords(from, fromInclusive, to, toInclusive);
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        return true;
    }

    @Override
    public String toString() {
        return Arrays.toString(components) + " in " + (fromInclusive ? "[" : "(") + from + ", " + to + (toInclusive ? "]" : ")");
    }

    private void generateFallbackPredicate() {
        // here we are reversing the work of CompositeIndexVisitor.generateRangePredicate

        Comparable[] fromValues = from.getComponents();
        Comparable[] toValues = to.getComponents();
        Comparable comparisonFrom = fromValues[prefixLength];
        Comparable comparisonTo = toValues[prefixLength];
        boolean hasComparison = isFinite(comparisonFrom) || isFinite(comparisonTo);

        Predicate[] predicates = new Predicate[hasComparison ? prefixLength + 1 : prefixLength];
        for (int i = 0; i < prefixLength; ++i) {
            assert fromValues[i] == toValues[i];
            predicates[i] = new EqualPredicate(components[i], fromValues[i]);
        }

        if (hasComparison) {
            String comparisonComponent = components[prefixLength];
            boolean comparisonFromInclusive =
                    fromInclusive || prefixLength < components.length - 1 && fromValues[prefixLength + 1] == NEGATIVE_INFINITY;
            boolean comparisonToInclusive =
                    toInclusive || prefixLength < components.length - 1 && toValues[prefixLength + 1] == POSITIVE_INFINITY;

            if (isFinite(comparisonFrom) && isFinite(comparisonTo)) {
                predicates[prefixLength] =
                        new BoundedRangePredicate(comparisonComponent, comparisonFrom, comparisonFromInclusive, comparisonTo,
                                comparisonToInclusive);
            } else if (isFinite(comparisonFrom)) {
                predicates[prefixLength] =
                        new GreaterLessPredicate(comparisonComponent, comparisonFrom, comparisonFromInclusive, false);
            } else {
                predicates[prefixLength] =
                        new GreaterLessPredicate(comparisonComponent, comparisonTo, comparisonToInclusive, true);
            }
        }

        fallbackPredicate = new AndPredicate(predicates);
    }

    private void writeObject(ObjectOutputStream stream) throws IOException {
        throw new UnsupportedOperationException("can't be serialized");
    }

    private static boolean isFinite(Comparable value) {
        // Null is serving as a second negative infinity that is greater than
        // the regular one. See CompositeValue docs for more details.
        return value != NULL && value != NEGATIVE_INFINITY && value != POSITIVE_INFINITY;
    }

}
