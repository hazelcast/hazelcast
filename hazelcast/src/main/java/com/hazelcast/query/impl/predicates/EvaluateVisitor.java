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

import com.hazelcast.core.TypeConverter;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryContext.IndexMatchHint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.query.impl.Indexes.SKIP_PARTITIONS_COUNT_CHECK;

/**
 * Tries to divide the predicate tree into isolated subtrees every of which can
 * be evaluated by {@link Index#evaluate} method in a single go.
 */
public class EvaluateVisitor extends AbstractVisitor {

    private static final Predicate[] EMPTY_PREDICATES = new Predicate[0];

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    @Override
    public Predicate visit(AndPredicate andPredicate, Indexes indexes) {
        Predicate[] predicates = andPredicate.predicates;

        // Try to group evaluable predicates by their indexes.

        Map<String, List<EvaluatePredicate>> evaluable = null;
        boolean requiresGeneration = false;
        for (Predicate subPredicate : predicates) {
            if (!(subPredicate instanceof EvaluatePredicate)) {
                continue;
            }

            EvaluatePredicate evaluatePredicate = (EvaluatePredicate) subPredicate;
            String indexName = evaluatePredicate.getIndexName();
            Index index = indexes.matchIndex(indexName, andPredicate.getClass(), IndexMatchHint.EXACT_NAME,
                    SKIP_PARTITIONS_COUNT_CHECK);
            if (index == null) {
                continue;
            }

            if (evaluable == null) {
                evaluable = new HashMap<>(predicates.length);
            }
            List<EvaluatePredicate> group = evaluable.get(indexName);
            if (group == null) {
                group = new ArrayList<>(predicates.length);
                evaluable.put(indexName, group);
            } else {
                requiresGeneration = true;
            }
            group.add(evaluatePredicate);
        }

        if (!requiresGeneration) {
            // no changes to the predicates required
            return andPredicate;
        }

        // Add non-evaluable predicates to the output.

        List<Predicate> output = new ArrayList<>();
        for (Predicate subPredicate : predicates) {
            if (!(subPredicate instanceof EvaluatePredicate)) {
                output.add(subPredicate);
                continue;
            }

            EvaluatePredicate evaluatePredicate = (EvaluatePredicate) subPredicate;
            String indexName = evaluatePredicate.getIndexName();
            Index index = indexes.matchIndex(indexName, andPredicate.getClass(), IndexMatchHint.EXACT_NAME,
                    SKIP_PARTITIONS_COUNT_CHECK);
            if (index == null) {
                output.add(subPredicate);
            }
        }

        // Add evaluable predicates to the output.

        for (Map.Entry<String, List<EvaluatePredicate>> groupEntry : evaluable.entrySet()) {
            String indexName = groupEntry.getKey();
            List<EvaluatePredicate> group = groupEntry.getValue();

            if (group.size() == 1) {
                output.add(group.get(0));
                continue;
            }

            Predicate[] groupPredicates = new Predicate[group.size()];
            for (int i = 0; i < groupPredicates.length; ++i) {
                groupPredicates[i] = group.get(i).getPredicate();
            }
            output.add(new EvaluatePredicate(new AndPredicate(groupPredicates), indexName));
        }

        return output.size() == 1 ? output.get(0) : new AndPredicate(output.toArray(EMPTY_PREDICATES));
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    @Override
    public Predicate visit(OrPredicate orPredicate, Indexes indexes) {
        Predicate[] predicates = orPredicate.predicates;

        // Try to group evaluable predicates by their indexes.

        Map<String, List<EvaluatePredicate>> evaluable = null;
        boolean requiresGeneration = false;
        for (Predicate subPredicate : predicates) {
            if (!(subPredicate instanceof EvaluatePredicate)) {
                continue;
            }

            EvaluatePredicate evaluatePredicate = (EvaluatePredicate) subPredicate;
            String indexName = evaluatePredicate.getIndexName();
            Index index = indexes.matchIndex(indexName, orPredicate.getClass(), IndexMatchHint.EXACT_NAME,
                    SKIP_PARTITIONS_COUNT_CHECK);
            if (index == null) {
                continue;
            }

            if (evaluable == null) {
                evaluable = new HashMap<>(predicates.length);
            }
            List<EvaluatePredicate> group = evaluable.get(indexName);
            if (group == null) {
                group = new ArrayList<>(predicates.length);
                evaluable.put(indexName, group);
            } else {
                requiresGeneration = true;
            }
            group.add(evaluatePredicate);
        }

        if (!requiresGeneration) {
            // no changes to the predicates required
            return orPredicate;
        }

        // Add non-evaluable predicates to the output.

        List<Predicate> output = new ArrayList<>();
        for (Predicate subPredicate : predicates) {
            if (!(subPredicate instanceof EvaluatePredicate)) {
                output.add(subPredicate);
                continue;
            }

            EvaluatePredicate evaluatePredicate = (EvaluatePredicate) subPredicate;
            String indexName = evaluatePredicate.getIndexName();
            Index index = indexes.matchIndex(indexName, orPredicate.getClass(), IndexMatchHint.EXACT_NAME,
                    SKIP_PARTITIONS_COUNT_CHECK);
            if (index == null) {
                output.add(subPredicate);
            }
        }

        // Add evaluable predicates to the output.

        for (Map.Entry<String, List<EvaluatePredicate>> groupEntry : evaluable.entrySet()) {
            String indexName = groupEntry.getKey();
            List<EvaluatePredicate> group = groupEntry.getValue();

            if (group.size() == 1) {
                output.add(group.get(0));
                continue;
            }

            Predicate[] groupPredicates = new Predicate[group.size()];
            for (int i = 0; i < groupPredicates.length; ++i) {
                groupPredicates[i] = group.get(i).getPredicate();
            }
            output.add(new EvaluatePredicate(new OrPredicate(groupPredicates), indexName));
        }

        return output.size() == 1 ? output.get(0) : new OrPredicate(output.toArray(EMPTY_PREDICATES));
    }

    @Override
    public Predicate visit(NotPredicate notPredicate, Indexes indexes) {
        Predicate subPredicate = notPredicate.getPredicate();
        if (!(subPredicate instanceof EvaluatePredicate)) {
            return notPredicate;
        }

        EvaluatePredicate evaluatePredicate = (EvaluatePredicate) subPredicate;
        String indexName = evaluatePredicate.getIndexName();
        Index index = indexes.matchIndex(indexName, notPredicate.getClass(), IndexMatchHint.EXACT_NAME,
                SKIP_PARTITIONS_COUNT_CHECK);
        if (index == null) {
            return notPredicate;
        }

        return new EvaluatePredicate(new NotPredicate(evaluatePredicate.getPredicate()), indexName);
    }

    @Override
    public Predicate visit(EqualPredicate predicate, Indexes indexes) {
        Index index = indexes.matchIndex(predicate.attributeName, predicate.getClass(), IndexMatchHint.PREFER_UNORDERED,
                SKIP_PARTITIONS_COUNT_CHECK);
        if (index == null) {
            return predicate;
        }

        TypeConverter converter = index.getConverter();
        if (converter == null) {
            return predicate;
        }

        return new EvaluatePredicate(predicate, index.getName());
    }

    @Override
    public Predicate visit(NotEqualPredicate predicate, Indexes indexes) {
        Index index = indexes.matchIndex(predicate.attributeName, predicate.getClass(), IndexMatchHint.PREFER_UNORDERED,
                SKIP_PARTITIONS_COUNT_CHECK);
        if (index == null) {
            return predicate;
        }

        TypeConverter converter = index.getConverter();
        if (converter == null) {
            return predicate;
        }

        return new EvaluatePredicate(predicate, index.getName());
    }

    @Override
    public Predicate visit(InPredicate predicate, Indexes indexes) {
        Index index = indexes.matchIndex(predicate.attributeName, predicate.getClass(), IndexMatchHint.PREFER_UNORDERED,
                SKIP_PARTITIONS_COUNT_CHECK);
        if (index == null) {
            return predicate;
        }

        TypeConverter converter = index.getConverter();
        if (converter == null) {
            return predicate;
        }

        return new EvaluatePredicate(predicate, index.getName());
    }

}
