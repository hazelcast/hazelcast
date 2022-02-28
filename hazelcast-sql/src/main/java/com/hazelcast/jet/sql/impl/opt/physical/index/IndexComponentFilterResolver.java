/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.opt.physical.index;

import com.hazelcast.config.IndexType;
import com.hazelcast.sql.impl.exec.scan.index.IndexCompositeFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilterValue;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static com.hazelcast.config.IndexType.SORTED;
import static java.util.Collections.singletonList;

final class IndexComponentFilterResolver {
    private static final Predicate<IndexCompositeFilter> ONLY_EQUALS_FILTERS_PREDICATE = indexCompositeFilter ->
            indexCompositeFilter.getFilters().stream().allMatch(indexFilter -> indexFilter instanceof IndexEqualsFilter);
    private static final Predicate<IndexCompositeFilter> ALL_FILTERS_PREDICATE = indexFilter -> true;

    private IndexComponentFilterResolver() {
    }

    /**
     * This method selects the best expression to be used as index filter from the list of candidates.
     *
     * @param type          type of the index (SORTED, HASH)
     * @param candidates    candidates that might be used as a filter
     * @param converterType expected converter type for the given component of the index
     * @return filter for the index component or {@code null} if no candidate could be applied
     */
    static IndexComponentFilter findBestComponentFilter(
            IndexType type,
            List<IndexComponentCandidate> candidates,
            QueryDataType converterType
    ) {
        // First look for equality filters, assuming that they are more selective than ranges
        IndexComponentFilter equalityComponentFilter = searchForEquality(candidates, converterType);
        if (equalityComponentFilter != null) {
            return equalityComponentFilter;
        }

        // Look for ranges filters
        return searchForRange(type, candidates, converterType);
    }

    private static IndexComponentFilter searchForEquality(
            List<IndexComponentCandidate> candidates,
            QueryDataType converterType
    ) {
        // First look for a single equality condition, assuming that it is the most selective
        IndexComponentFilter candidate = convertFromEqualsFilter(candidates, converterType);
        if (candidate != null) {
            return candidate;
        }

        // Next look for composite (IN (a,b) like), as it is worse than equality on a single value, but better than range.
        // We choose only composite containing equals filters only here, since index may not be SORTED.
        return convertFromCompositeFilter(candidates, converterType, ONLY_EQUALS_FILTERS_PREDICATE);
    }

    private static IndexComponentFilter searchForRange(
            IndexType type,
            List<IndexComponentCandidate> candidates,
            QueryDataType converterType
    ) {
        if (type != SORTED) {
            return null;
        }

        // Looking for a filter merged from one or many range filters.
        IndexComponentFilter filter = convertFromRangeFilters(candidates, converterType);
        if (filter != null) {
            return filter;
        }

        // Last place to look, composite filter with at least one range. This one may contain both ranges and equalities.
        return convertFromCompositeFilter(candidates, converterType, ALL_FILTERS_PREDICATE);
    }

    private static IndexComponentFilter convertFromEqualsFilter(
            List<IndexComponentCandidate> candidates,
            QueryDataType converterType
    ) {
        for (IndexComponentCandidate candidate : candidates) {
            if (!(candidate.getFilter() instanceof IndexEqualsFilter)) {
                continue;
            }

            return new IndexComponentFilter(
                    candidate.getFilter(),
                    singletonList(candidate.getExpression()),
                    converterType
            );
        }
        return null;
    }

    private static IndexComponentFilter convertFromRangeFilters(
            List<IndexComponentCandidate> candidates,
            QueryDataType converterType
    ) {
        IndexFilterValue from = null;
        boolean fromInclusive = false;
        IndexFilterValue to = null;
        boolean toInclusive = false;
        List<RexNode> expressions = new ArrayList<>(2);

        for (IndexComponentCandidate candidate : candidates) {
            if (!(candidate.getFilter() instanceof IndexRangeFilter)) {
                continue;
            }

            IndexRangeFilter candidateFilter = (IndexRangeFilter) candidate.getFilter();

            if (from == null && candidateFilter.getFrom() != null) {
                from = candidateFilter.getFrom();
                fromInclusive = candidateFilter.isFromInclusive();
                expressions.add(candidate.getExpression());
            }

            if (to == null && candidateFilter.getTo() != null) {
                to = candidateFilter.getTo();
                toInclusive = candidateFilter.isToInclusive();
                expressions.add(candidate.getExpression());
            }
        }

        if (from != null || to != null) {
            IndexRangeFilter filter = new IndexRangeFilter(from, fromInclusive, to, toInclusive);
            return new IndexComponentFilter(filter, expressions, converterType);
        }

        return null;
    }

    private static IndexComponentFilter convertFromCompositeFilter(
            List<IndexComponentCandidate> candidates,
            QueryDataType converterType,
            Predicate<IndexCompositeFilter> additionalFilter
    ) {
        for (IndexComponentCandidate candidate : candidates) {
            if (!(candidate.getFilter() instanceof IndexCompositeFilter)) {
                continue;
            }

            if (!additionalFilter.test((IndexCompositeFilter) candidate.getFilter())) {
                continue;
            }

            return new IndexComponentFilter(
                    candidate.getFilter(),
                    singletonList(candidate.getExpression()),
                    converterType
            );
        }

        return null;
    }
}
