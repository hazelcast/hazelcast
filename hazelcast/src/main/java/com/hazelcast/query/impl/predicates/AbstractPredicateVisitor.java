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

import static com.hazelcast.util.collection.ArrayUtils.createCopy;

/**
 * Base class for all visitors. It returns the original predicate without touching.
 * Concrete PredicateVisitor can just override the method for predicate the visitor is interested in.
 */
public abstract class AbstractPredicateVisitor<T> implements PredicateVisitor<Predicate> {


    public static Predicate[] visitAll(Predicate[] predicates, PredicateVisitor visitor) {
        Predicate[] target = predicates;
        boolean copyCreated = false;
        for (int i = 0; i < predicates.length; i++) {
            Predicate predicate = predicates[i];
            if (predicate instanceof VisitablePredicate) {
                Predicate transformed = (Predicate) ((VisitablePredicate) predicate).visit(visitor);
                if (transformed != predicate) {
                    if (!copyCreated) {
                        copyCreated = true;
                        target = createCopy(target);
                    }
                    target[i] = transformed;
                }
            }
        }
        return target;
    }

    @Override
    public Predicate visit(AndPredicate predicate) {
        Predicate[] result = visitAll(predicate.predicates, this);
        return result == predicate.predicates ? predicate : new AndPredicate(result);
    }

    @Override
    public Predicate visit(OrPredicate predicate) {
        Predicate[] result = visitAll(predicate.predicates, this);
        return result == predicate.predicates ? predicate : new OrPredicate(result);
    }

    @Override
    public Predicate visit(NotPredicate predicate) {
        if (!(predicate.predicate instanceof VisitablePredicate)) {
            return predicate;
        }

        Predicate result = ((VisitablePredicate) predicate.predicate).visit(this);
        return result == predicate.predicate ? predicate : new NotPredicate(result);
    }

    @Override
    public Predicate visit(BetweenPredicate predicate) {
        return visitDefault(predicate);
    }

    @Override
    public Predicate visit(EqualPredicate predicate) {
        return visitDefault(predicate);
    }

    @Override
    public Predicate visit(NotEqualPredicate predicate) {
        return visitDefault(predicate);
    }

    @Override
    public Predicate visit(GreaterLessPredicate predicate) {
        return visitDefault(predicate);
    }

    @Override
    public Predicate visit(InPredicate predicate) {
        return visitDefault(predicate);
    }

    @Override
    public Predicate visit(InstanceOfPredicate predicate) {
        return visitDefault(predicate);
    }

    @Override
    public Predicate visit(LikePredicate predicate) {
        return visitDefault(predicate);
    }

    @Override
    public Predicate visit(RegexPredicate predicate) {
        return visitDefault(predicate);
    }

    @Override
    public Predicate visit(IndexSupressionPredicate predicate) {
        return visitDefault(predicate);
    }

    public Predicate visitDefault(VisitablePredicate p) {
        return this;
    }
}
