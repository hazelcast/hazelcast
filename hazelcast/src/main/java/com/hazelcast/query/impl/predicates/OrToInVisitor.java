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
import com.hazelcast.internal.util.collection.InternalListMultiMap;

import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.collection.ArrayUtils.createCopy;

/**
 * Transforms predicate (attribute = 1 or attribute = 2 or attribute = 3) into
 * (attribute in (1, 2, 3)
 *
 * InPredicate is easier to evaluate in both indexed and non-indexed scenarios.
 *
 * When index is not used then {@link Predicate} require just a single reflective call
 * the extract an attribute for {@link com.hazelcast.query.impl.QueryableEntry} while
 * disjunction of equalPredicate(s) requires one reflective call for each equalPredicate.
 *
 * The performance is even more significant when tha {@link InPredicate#attributeName} is indexed.
 * As then the InPrecicate can be evaluated by just a single hit into index.
 *
 *
 * When index is used then
 *
 *
 */
public class OrToInVisitor extends AbstractVisitor {

    private static final int MINIMUM_NUMBER_OF_OR_TO_REPLACE = 5;

    @Override
    public Predicate visit(OrPredicate orPredicate, Indexes indexes) {
        Predicate[] originalInnerPredicates = orPredicate.predicates;
        if (originalInnerPredicates == null || originalInnerPredicates.length < MINIMUM_NUMBER_OF_OR_TO_REPLACE) {
            return orPredicate;
        }

        InternalListMultiMap<String, Integer> candidates = findAndGroupCandidates(originalInnerPredicates);
        if (candidates == null) {
            return orPredicate;
        }
        int toBeRemoved = 0;
        boolean modified = false;
        Predicate[] target = originalInnerPredicates;
        for (Map.Entry<String, List<Integer>> candidate : candidates.entrySet()) {
            String attribute = candidate.getKey();
            List<Integer> positions = candidate.getValue();
            if (positions.size() < MINIMUM_NUMBER_OF_OR_TO_REPLACE) {
                continue;
            }
            if (!modified) {
                modified = true;
                target = createCopy(target);
            }
            toBeRemoved = replaceForAttribute(attribute, target, positions, toBeRemoved);
        }
        Predicate[] newInnerPredicates = replaceInnerPredicates(target, toBeRemoved);
        return getOrCreateFinalPredicate(orPredicate, originalInnerPredicates, newInnerPredicates);
    }

    private Predicate getOrCreateFinalPredicate(OrPredicate predicate, Predicate[] innerPredicates, Predicate[] newPredicates) {
        if (newPredicates == innerPredicates) {
            return predicate;
        }
        if (newPredicates.length == 1) {
            return newPredicates[0];
        }
        return new OrPredicate(newPredicates);
    }

    private int replaceForAttribute(String attribute, Predicate[] innerPredicates, List<Integer> positions, int toBeRemoved) {
        Comparable[] values = new Comparable[positions.size()];
        for (int i = 0; i < positions.size(); i++) {
            int position = positions.get(i);
            EqualPredicate equalPredicate = ((EqualPredicate) innerPredicates[position]);
            values[i] = equalPredicate.value;
            innerPredicates[position] = null;
            toBeRemoved++;
        }
        InPredicate inPredicate = new InPredicate(attribute, values);
        innerPredicates[positions.get(0)] = inPredicate;
        toBeRemoved--;
        return toBeRemoved;
    }

    private Predicate[] replaceInnerPredicates(Predicate[] innerPredicates, int toBeRemoved) {
        if (toBeRemoved == 0) {
            return innerPredicates;
        }

        int removed = 0;
        int newSize = innerPredicates.length - toBeRemoved;
        Predicate[] newPredicates = new Predicate[newSize];
        for (int i = 0; i < innerPredicates.length; i++) {
            Predicate p = innerPredicates[i];
            if (p != null) {
                newPredicates[i - removed] = p;
            } else {
                removed++;
            }
        }
        return newPredicates;
    }

    private InternalListMultiMap<String, Integer> findAndGroupCandidates(Predicate[] innerPredicates) {
        InternalListMultiMap<String, Integer> candidates = null;
        for (int i = 0; i < innerPredicates.length; i++) {
            Predicate p = innerPredicates[i];
            if (p.getClass().equals(EqualPredicate.class)) {
                EqualPredicate equalPredicate = (EqualPredicate) p;
                String attribute = equalPredicate.attributeName;
                if (candidates == null) {
                    candidates = new InternalListMultiMap<String, Integer>();
                }
                candidates.put(attribute, i);
            }
        }
        return candidates;
    }
}
