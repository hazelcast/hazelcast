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
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EvaluateVisitor extends AbstractVisitor {

    @Override
    public Predicate visit(EqualPredicate predicate, Indexes indexes) {
        Index index = indexes.matchIndex(predicate.attributeName, QueryContext.IndexMatchHint.PREFER_UNORDERED);
        if (index == null) {
            return predicate;
        }

        if (!index.canEvaluate(predicate.getClass())) {
            return predicate;
        }

        TypeConverter converter = indexes.getConverter(predicate.attributeName);
        if (converter == null) {
            return predicate;
        }

        return new EvaluatePredicate(predicate, index.getName(), converter);
    }

    @Override
    public Predicate visit(NotEqualPredicate predicate, Indexes indexes) {
        Index index = indexes.matchIndex(predicate.attributeName, QueryContext.IndexMatchHint.PREFER_UNORDERED);
        if (index == null) {
            return predicate;
        }

        if (!index.canEvaluate(predicate.getClass())) {
            return predicate;
        }

        TypeConverter converter = indexes.getConverter(predicate.attributeName);
        if (converter == null) {
            return predicate;
        }

        return new EvaluatePredicate(predicate, index.getName(), converter);
    }

    @Override
    public Predicate visit(AndPredicate predicate, Indexes indexes) {
        // TODO optimize

        List<Predicate> output = new ArrayList<Predicate>(predicate.predicates.length);
        Map<String, List<EvaluatePredicate>> evaluable =
                new HashMap<String, List<EvaluatePredicate>>(predicate.predicates.length);

        for (Predicate subPredicate : predicate.predicates) {
            if (!(subPredicate instanceof EvaluatePredicate)) {
                output.add(subPredicate);
                continue;
            }

            EvaluatePredicate evaluatePredicate = (EvaluatePredicate) subPredicate;
            List<EvaluatePredicate> group = evaluable.get(evaluatePredicate.getIndexName());
            if (group == null) {
                group = new ArrayList<EvaluatePredicate>(predicate.predicates.length);
                evaluable.put(evaluatePredicate.getIndexName(), group);
            }
            group.add(evaluatePredicate);
        }

        for (Map.Entry<String, List<EvaluatePredicate>> groupEntry : evaluable.entrySet()) {
            List<EvaluatePredicate> group = groupEntry.getValue();
            if (group.size() == 1) {
                output.add(group.get(0));
                continue;
            }

            Predicate[] groupPredicates = new Predicate[group.size()];
            for (int i = 0; i < groupPredicates.length; ++i) {
                groupPredicates[i] = group.get(i).getPredicate();
            }
            output.add(
                    new EvaluatePredicate(new AndPredicate(groupPredicates), groupEntry.getKey(), group.get(0).getConverter()));
        }

        return output.size() == 1 ? output.get(0) : new AndPredicate(output.toArray(new Predicate[0]));
    }

    @Override
    public Predicate visit(OrPredicate predicate, Indexes indexes) {
        // TODO optimize

        List<Predicate> output = new ArrayList<Predicate>(predicate.predicates.length);
        Map<String, List<EvaluatePredicate>> evaluable =
                new HashMap<String, List<EvaluatePredicate>>(predicate.predicates.length);

        for (Predicate subPredicate : predicate.predicates) {
            if (!(subPredicate instanceof EvaluatePredicate)) {
                output.add(subPredicate);
                continue;
            }

            EvaluatePredicate evaluatePredicate = (EvaluatePredicate) subPredicate;
            List<EvaluatePredicate> group = evaluable.get(evaluatePredicate.getIndexName());
            if (group == null) {
                group = new ArrayList<EvaluatePredicate>(predicate.predicates.length);
                evaluable.put(evaluatePredicate.getIndexName(), group);
            }
            group.add(evaluatePredicate);
        }

        for (Map.Entry<String, List<EvaluatePredicate>> groupEntry : evaluable.entrySet()) {
            List<EvaluatePredicate> group = groupEntry.getValue();
            if (group.size() == 1) {
                output.add(group.get(0));
                continue;
            }

            Predicate[] groupPredicates = new Predicate[group.size()];
            for (int i = 0; i < groupPredicates.length; ++i) {
                groupPredicates[i] = group.get(i).getPredicate();
            }
            output.add(new EvaluatePredicate(new OrPredicate(groupPredicates), groupEntry.getKey(), group.get(0).getConverter()));
        }

        return output.size() == 1 ? output.get(0) : new OrPredicate(output.toArray(new Predicate[0]));
    }

    @Override
    public Predicate visit(InPredicate predicate, Indexes indexes) {
        Index index = indexes.matchIndex(predicate.attributeName, QueryContext.IndexMatchHint.PREFER_UNORDERED);
        if (index == null) {
            return predicate;
        }

        if (!index.canEvaluate(predicate.getClass())) {
            return predicate;
        }

        TypeConverter converter = indexes.getConverter(predicate.attributeName);
        if (converter == null) {
            return predicate;
        }

        return new EvaluatePredicate(predicate, index.getName(), converter);
    }

}
