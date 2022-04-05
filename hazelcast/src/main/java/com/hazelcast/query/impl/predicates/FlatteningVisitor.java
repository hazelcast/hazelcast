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

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.collection.ArrayUtils.createCopy;

/**
 * Rewrites predicates:
 *
 * 1. AndPredicates flattening:
 * (a = 1 and (b = 2 and c = 3)) into (a = 1 and b = 2 and c = 3)
 *
 * 2. OrPredicate flattening:
 * (a = 1 or (b = 2 or c = 3)) into (a = 1 or b = 2 or c = 3)
 *
 * 3. NotElimination
 * (not(P)) when P is {@link NegatablePredicate} is rewritten as P.negate()
 *
 *
 */
public class FlatteningVisitor extends AbstractVisitor {

    @Override
    public Predicate visit(AndPredicate andPredicate, Indexes indexes) {
        Predicate[] originalPredicates = andPredicate.predicates;
        List<Predicate> toBeAdded = null;
        boolean modified = false;
        Predicate[] target = originalPredicates;
        for (int i = 0; i < target.length; i++) {
            Predicate predicate = target[i];
            if (predicate instanceof AndPredicate) {
                Predicate[] subPredicates = ((AndPredicate) predicate).predicates;
                if (!modified) {
                    modified = true;
                    target = createCopy(target);
                }
                toBeAdded = replaceFirstAndStoreOthers(target, subPredicates, i, toBeAdded);
            }
        }
        Predicate[] newInners = createNewInners(target, toBeAdded);
        if (newInners == originalPredicates) {
            return andPredicate;
        }
        return new AndPredicate(newInners);
    }

    @Override
    public Predicate visit(OrPredicate orPredicate, Indexes indexes) {
        Predicate[] originalPredicates = orPredicate.predicates;
        List<Predicate> toBeAdded = null;
        boolean modified = false;
        Predicate[] target = originalPredicates;
        for (int i = 0; i < target.length; i++) {
            Predicate predicate = target[i];
            if (predicate instanceof OrPredicate) {
                Predicate[] subPredicates = ((OrPredicate) predicate).predicates;
                if (!modified) {
                    modified = true;
                    target = createCopy(target);
                }
                toBeAdded = replaceFirstAndStoreOthers(target, subPredicates, i, toBeAdded);
            }
        }
        Predicate[] newInners = createNewInners(target, toBeAdded);
        if (newInners == originalPredicates) {
            return orPredicate;
        }
        return new OrPredicate(newInners);
    }

    private List<Predicate> replaceFirstAndStoreOthers(Predicate[] predicates, Predicate[] subPredicates,
                                                       int position, List<Predicate> store) {
        if (subPredicates == null || subPredicates.length == 0) {
            return store;
        }

        predicates[position] = subPredicates[0];
        for (int j = 1; j < subPredicates.length; j++) {
            if (store == null) {
                store = new ArrayList<Predicate>();
            }
            store.add(subPredicates[j]);
        }
        return store;
    }

    private Predicate[] createNewInners(Predicate[] predicates, List<Predicate> toBeAdded) {
        if (toBeAdded == null || toBeAdded.size() == 0) {
            return predicates;
        }

        int newSize = predicates.length + toBeAdded.size();
        Predicate[] newPredicates = new Predicate[newSize];
        System.arraycopy(predicates, 0, newPredicates, 0, predicates.length);
        for (int i = predicates.length; i < newSize; i++) {
            newPredicates[i] = toBeAdded.get(i - predicates.length);
        }
        return newPredicates;
    }

    @Override
    public Predicate visit(NotPredicate predicate, Indexes indexes) {
        Predicate inner = predicate.predicate;
        if (inner instanceof NegatablePredicate) {
            return ((NegatablePredicate) inner).negate();
        }
        return predicate;
    }

}
