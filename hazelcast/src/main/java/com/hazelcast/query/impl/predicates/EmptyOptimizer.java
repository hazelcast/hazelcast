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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.VisitablePredicate;
import com.hazelcast.query.impl.Indexes;

/**
 * Optimizer which just returns the original predicate.
 * It's useful when optimizer is disabled.
 */
public class EmptyOptimizer extends AbstractPredicateVisitor implements QueryOptimizer {

    @Override
    public <K, V> Predicate<K, V> optimize(Predicate<K, V> predicate, Indexes indexes) {
        if (predicate instanceof VisitablePredicate) {
            return (Predicate) ((VisitablePredicate) predicate).visit(this);
        } else {
            return predicate;
        }
    }

    @Override
    public Predicate visit(BetweenPredicate predicate) {
        if (predicate.attributeName.startsWith("%-")) {
            return new BetweenPredicate(predicate.attributeName.substring(2), predicate.from, predicate.to);
        } else {
            return predicate;
        }
    }

    @Override
    public Predicate visit(EqualPredicate predicate) {
        return super.visit(predicate);
    }

    @Override
    public Predicate visit(NotEqualPredicate predicate) {
        return super.visit(predicate);
    }

    @Override
    public Predicate visit(GreaterLessPredicate predicate) {
        return super.visit(predicate);
    }

    @Override
    public Predicate visit(InPredicate predicate) {
        return super.visit(predicate);
    }
}
