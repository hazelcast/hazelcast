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
import com.hazelcast.query.impl.Indexes;

/**
 * Rule based optimizer. It chains {@link Visitor}s to rewrite query.
 */
public final class RuleBasedQueryOptimizer implements QueryOptimizer {

    private final Visitor flatteningVisitor = new FlatteningVisitor();
    private final Visitor rangeVisitor = new RangeVisitor();
    private final Visitor orToInVisitor = new OrToInVisitor();
    private final Visitor compositeIndexVisitor = new CompositeIndexVisitor();
    private final Visitor evaluateVisitor = new EvaluateVisitor();

    @SuppressWarnings("unchecked")
    public <K, V> Predicate<K, V> optimize(Predicate<K, V> predicate, Indexes indexes) {
        Predicate optimized = predicate;
        if (optimized instanceof VisitablePredicate) {
            optimized = ((VisitablePredicate) optimized).accept(flatteningVisitor, indexes);
        }
        if (optimized instanceof VisitablePredicate) {
            optimized = ((VisitablePredicate) optimized).accept(rangeVisitor, indexes);
        }
        if (optimized instanceof VisitablePredicate) {
            optimized = ((VisitablePredicate) optimized).accept(orToInVisitor, indexes);
        }
        if (optimized instanceof VisitablePredicate) {
            optimized = ((VisitablePredicate) optimized).accept(compositeIndexVisitor, indexes);
        }
        if (optimized instanceof VisitablePredicate) {
            optimized = ((VisitablePredicate) optimized).accept(evaluateVisitor, indexes);
        }
        return optimized;
    }

}
