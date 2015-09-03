/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.TypeConverters;
import com.hazelcast.util.collection.ArrayUtils;
import com.hazelcast.util.collection.InternalMultiMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.FalsePredicate;

import java.util.List;
import java.util.Map;

import static com.hazelcast.util.collection.ArrayUtils.createCopy;

/**
 * Replaces expression from (age >= X and age <= Y) into (age between X Y)
 * It detects some predicates which are trivally false.
 *
 * Imagine this: (age >= 5 and age <= 4) This predicate is always false.
 *
 * It also eliminates some conditions. Imagine this: (age >= 10 and age <= 20 and age < 40).
 * In this case the condition (age < 40) can be eliminated safely and the predicate
 * be be rewritten as (age between 10 20)
 *
 * When resulting AndPredicate contains only a single inner predicate then it returns
 * only the inner predicate. In other words: (and p) is rewritten as (p). I used prefix
 * notation here as it's easier to understand.
 *
 */
public class BetweenVisitor extends AbstractVisitor {

    @Override
    public Predicate visit(AndPredicate andPredicate, Indexes indexes) {
        final Predicate[] originalPredicates = andPredicate.predicates;
        InternalMultiMap<String, GreaterLessPredicate> candidates =
                findCandidatesAndGroupByAttribute(originalPredicates, indexes);

        if (candidates == null) {
            return andPredicate;
        }

        //how many predicates is eliminated
        int toBeRemovedCounter = 0;
        boolean modified = false;
        Predicate[] target = originalPredicates;
        for (Map.Entry<String, List<GreaterLessPredicate>> entry : candidates.entrySet()) {
            List<GreaterLessPredicate> predicates = entry.getValue();
            if (predicates.size() == 1) {
                continue;
            }
            String attributeName = entry.getKey();
            Boundaries boundaries = findBoundaryOrNull(attributeName, predicates, indexes);
            if (boundaries == null) {
                //no boundaries were found. it's not in the form `foo >= X and foo <= Y`
                continue;
            }

            if (boundaries.isOverlapping()) {
                // the predicate is never true. imagine this: `foo >= 5 and foo <= 4`
                // it can never be true, we can't replace the whole AND Node with FalsePredicate

                // We can return FalsePredicate even if there are additional predicate
                // as `foo >= 5 and foo <= 5 and bar = 3` is still always false. the `bar = 3` can
                // be eliminated too.
                return FalsePredicate.INSTANCE;
            }
            if (!modified) {
                modified = true;
                target = createCopy(target);
            }
            toBeRemovedCounter = rewriteAttribute(boundaries, target, toBeRemovedCounter);
        }
        Predicate[] newPredicates = removeEliminatedPredicates(target, toBeRemovedCounter);
        if (newPredicates == originalPredicates) {
            return andPredicate;
        }
        if (newPredicates.length == 1) {
            return newPredicates[0];
        }
        return new AndPredicate(newPredicates);
    }

    private Boundaries findBoundaryOrNull(String attributeName,
                                          List<GreaterLessPredicate> predicates, Indexes indexes) {
        GreaterLessPredicate mostRightGreaterOrEquals = null;
        GreaterLessPredicate mostLeftLessThanOrEquals = null;
        Index index = indexes.getIndex(attributeName);
        TypeConverters.TypeConverter converter = index.getConverter();
        for (GreaterLessPredicate predicate : predicates) {
            if (predicate.less) {
                if (mostLeftLessThanOrEquals == null || isLessThan(mostLeftLessThanOrEquals, predicate, converter)) {
                    mostLeftLessThanOrEquals = predicate;
                }
            } else {
                if (mostRightGreaterOrEquals == null || isGreaterThan(mostRightGreaterOrEquals, predicate, converter)) {
                    mostRightGreaterOrEquals = predicate;
                }
            }
        }
        if (mostRightGreaterOrEquals == null || mostLeftLessThanOrEquals == null) {
            return null;
        }
        return new Boundaries(mostRightGreaterOrEquals, mostLeftLessThanOrEquals, converter);
    }

    private Predicate[] removeEliminatedPredicates(Predicate[] originalPredicates, int toBeRemoved) {
        if (toBeRemoved == 0) {
            return originalPredicates;
        }
        int newSize = originalPredicates.length - toBeRemoved;
        Predicate[] newPredicates = new Predicate[newSize];
        ArrayUtils.copyWithoutNulls(originalPredicates, newPredicates);
        return newPredicates;
    }


    private int rewriteAttribute(Boundaries boundaries,
                                 Predicate[] originalPredicates, int toBeRemovedCount) {
        GreaterLessPredicate leftBoundary = boundaries.leftBoundary;
        GreaterLessPredicate rightBoundary = boundaries.rightBoundary;

        Predicate rewritten = boundaries.createEquivalentPredicate();
        for (int i = 0; i < originalPredicates.length; i++) {
            Predicate currentPredicate = originalPredicates[i];
            if (currentPredicate == leftBoundary) {
                originalPredicates[i] = rewritten;
            } else if (currentPredicate == rightBoundary) {
                originalPredicates[i] = null;
                toBeRemovedCount++;
            } else if (boundaries.canBeEliminated(currentPredicate)) {
                originalPredicates[i] = null;
                toBeRemovedCount++;
            }
        }
        return toBeRemovedCount;
    }

    /**
     * Find GreaterLessPredicates with equal flag set to true and group them by attribute name
     */
    private InternalMultiMap<String, GreaterLessPredicate> findCandidatesAndGroupByAttribute(Predicate[] predicates,
                                                                                             Indexes indexService) {
        InternalMultiMap<String, GreaterLessPredicate> candidates = null;
        for (Predicate predicate : predicates) {
            if (!(predicate instanceof GreaterLessPredicate)) {
                continue;
            }
            GreaterLessPredicate greaterLessPredicate = (GreaterLessPredicate) predicate;
            if (!(greaterLessPredicate.equal)) {
                continue;
            }
            String attributeName = greaterLessPredicate.attribute;
            Index index = indexService.getIndex(attributeName);
            if (index == null || index.getConverter() == null) {
                continue;
            }
            candidates = addIntoCandidates(greaterLessPredicate, candidates);
        }
        return candidates;
    }

    private InternalMultiMap<String, GreaterLessPredicate> addIntoCandidates(
            GreaterLessPredicate predicate, InternalMultiMap<String, GreaterLessPredicate> currentCandidates) {
        if (currentCandidates == null) {
            currentCandidates = new InternalMultiMap<String, GreaterLessPredicate>();
        }
        String attributeName = predicate.attribute;
        currentCandidates.put(attributeName, predicate);
        return currentCandidates;
    }

    /**
     * Represent a boundary of series of GreatLessPredicates connected by AND
     *
     * Example:
     * (age >= 5 and age >= 6 and age <= 10)
     * has a left boundary (age >= 6)
     * and a right boundary (age <= 10)
     *
     */
    private class Boundaries {
        private final GreaterLessPredicate leftBoundary;
        private final GreaterLessPredicate rightBoundary;
        private final TypeConverters.TypeConverter typeConverter;

        Boundaries(GreaterLessPredicate leftBoundary, GreaterLessPredicate rightBoundary,
                   TypeConverters.TypeConverter converter) {
            this.leftBoundary = leftBoundary;
            this.rightBoundary = rightBoundary;
            this.typeConverter = converter;
        }

        /**
         * Overlapping boundaries has a form of (age >= 5 and age <= 4) - predicates with overlaps
         * are always evaluated as false.
         *
         * @return
         */
        boolean isOverlapping() {
            return (isGreaterThan(rightBoundary, leftBoundary, typeConverter));
        }

        Predicate createEquivalentPredicate() {
            String attributeName = leftBoundary.attribute;
            if (isSame()) {
                return new EqualPredicate(attributeName, leftBoundary.value);
            } else {
                return new BetweenPredicate(attributeName, leftBoundary.value, rightBoundary.value);
            }
        }

        boolean isSame() {
            return leftBoundary.value == rightBoundary.value;
        }

        private boolean canBeEliminated(Predicate predicate) {
            if (!(predicate instanceof GreaterLessPredicate)) {
                return false;
            }
            GreaterLessPredicate greaterLessPredicate = (GreaterLessPredicate) predicate;
            if (!greaterLessPredicate.attribute.equals(leftBoundary.attribute)) {
                return false;
            }

            if (greaterLessPredicate.less) {
                return isGreaterThan(rightBoundary, greaterLessPredicate, typeConverter);
            } else {
                return isLessThan(leftBoundary, greaterLessPredicate, typeConverter);
            }
        }
    }

    private boolean isGreaterThan(GreaterLessPredicate leftPredicate,
                                  GreaterLessPredicate rightPredicate, TypeConverters.TypeConverter converter) {
        Comparable rightValue = converter.convert(rightPredicate.value);
        Comparable leftValue = converter.convert(leftPredicate.value);
        return rightValue.compareTo(leftValue) > 0;
    }

    private boolean isLessThan(GreaterLessPredicate leftPredicate, GreaterLessPredicate rightPredicate,
                               TypeConverters.TypeConverter converter) {
        Comparable rightValue = converter.convert(rightPredicate.value);
        Comparable leftValue = converter.convert(leftPredicate.value);
        return rightValue.compareTo(leftValue) < 0;
    }


}
