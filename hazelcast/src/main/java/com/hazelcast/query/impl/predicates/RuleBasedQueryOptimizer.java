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

import com.hazelcast.core.TypeConverter;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.VisitablePredicate;
import com.hazelcast.query.impl.FalsePredicate;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.util.collection.ArrayUtils;
import com.hazelcast.util.collection.InternalListMultiMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.collection.ArrayUtils.createCopy;

/**
 * Rule based optimizer. It chains {@link PredicateVisitor}s to rewrite query.
 *
 */
public final class RuleBasedQueryOptimizer implements QueryOptimizer {

    // todo: the traversal is missing.
    @Override
    public <K, V> Predicate<K, V> optimize(Predicate<K, V> predicate, Indexes indexes) {
        Predicate optimized = predicate;

        if (optimized instanceof VisitablePredicate) {
            optimized = ((VisitablePredicate) optimized).visit(new FlatteningVisitor());
        }

        if (optimized instanceof VisitablePredicate) {
            optimized = ((VisitablePredicate) optimized).visit(new BetweenVisitor(indexes));
        }

        if (optimized instanceof VisitablePredicate) {
            optimized = ((VisitablePredicate) optimized).visit(new OrToInVisitor());
        }

        if (optimized instanceof VisitablePredicate) {
            optimized = ((VisitablePredicate) optimized).visit(new IndexSkipOptimizer());
        }

        return optimized;
    }

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
    public static class FlatteningVisitor extends AbstractPredicateVisitor {

        @Override
        public Predicate visit(AndPredicate andPredicate) {
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
        public Predicate visit(OrPredicate orPredicate) {
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
        public Predicate visit(NotPredicate predicate) {
            Predicate inner = predicate.predicate;
            if (inner instanceof NegatablePredicate) {
                return ((NegatablePredicate) inner).negate();
            }
            return predicate;
        }

    }

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
    public static class OrToInVisitor extends AbstractPredicateVisitor {

        private static final int MINIMUM_NUMBER_OF_OR_TO_REPLACE = 5;

        @Override
        public Predicate visit(OrPredicate orPredicate) {
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
    public static class BetweenVisitor extends AbstractPredicateVisitor {

        private final Indexes indexes;

        public BetweenVisitor(Indexes indexes) {
            this.indexes = indexes;
        }

        @Override
        public Object visitDefault(VisitablePredicate p) {
            return p;
        }

        @Override
        public Predicate visit(AndPredicate andPredicate) {
            final Predicate[] originalPredicates = andPredicate.predicates;
            InternalListMultiMap<String, GreaterLessPredicate> candidates = findCandidatesAndGroupByAttribute(originalPredicates, indexes);

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
            TypeConverter converter = index.getConverter();
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
        private InternalListMultiMap<String, GreaterLessPredicate> findCandidatesAndGroupByAttribute(Predicate[] predicates,
                                                                                                     Indexes indexService) {
            InternalListMultiMap<String, GreaterLessPredicate> candidates = null;
            for (Predicate predicate : predicates) {
                if (!(predicate instanceof GreaterLessPredicate)) {
                    continue;
                }
                GreaterLessPredicate greaterLessPredicate = (GreaterLessPredicate) predicate;
                if (!(greaterLessPredicate.equal)) {
                    continue;
                }
                String attributeName = greaterLessPredicate.attributeName;
                Index index = indexService.getIndex(attributeName);
                if (index == null || index.getConverter() == null) {
                    continue;
                }
                candidates = addIntoCandidates(greaterLessPredicate, candidates);
            }
            return candidates;
        }

        private InternalListMultiMap<String, GreaterLessPredicate> addIntoCandidates(
                GreaterLessPredicate predicate, InternalListMultiMap<String, GreaterLessPredicate> currentCandidates) {
            if (currentCandidates == null) {
                currentCandidates = new InternalListMultiMap<String, GreaterLessPredicate>();
            }
            String attributeName = predicate.attributeName;
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
            private final TypeConverter typeConverter;

            Boundaries(GreaterLessPredicate leftBoundary, GreaterLessPredicate rightBoundary,
                       TypeConverter converter) {
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
                String attributeName = leftBoundary.attributeName;
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
                if (!greaterLessPredicate.attributeName.equals(leftBoundary.attributeName)) {
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
                                      GreaterLessPredicate rightPredicate, TypeConverter converter) {
            Comparable rightValue = converter.convert(rightPredicate.value);
            Comparable leftValue = converter.convert(leftPredicate.value);
            return rightValue.compareTo(leftValue) > 0;
        }

        private boolean isLessThan(GreaterLessPredicate leftPredicate, GreaterLessPredicate rightPredicate,
                                   TypeConverter converter) {
            Comparable rightValue = converter.convert(rightPredicate.value);
            Comparable leftValue = converter.convert(leftPredicate.value);
            return rightValue.compareTo(leftValue) < 0;
        }


    }

    public static class IndexSkipOptimizer extends AbstractPredicateVisitor  implements QueryOptimizer{

        @Override
        public <K, V> Predicate<K, V> optimize(Predicate<K, V> predicate, Indexes indexes) {
            if (predicate instanceof VisitablePredicate) {
                return (Predicate)((VisitablePredicate) predicate).visit(this);
            } else {
                return predicate;
            }
        }

        @Override
        public Predicate visit(BetweenPredicate predicate) {
            if (predicate.attributeName.startsWith("%-")) {
                BetweenPredicate p = new BetweenPredicate(predicate.attributeName.substring(2), predicate.from, predicate.to);
                return new IndexSupressionPredicate(p);
            } else {
                return predicate;
            }
        }

        @Override
        public Predicate visit(EqualPredicate predicate) {
            if (predicate.attributeName.startsWith("%-")) {
                EqualPredicate p = new EqualPredicate(predicate.attributeName.substring(2), predicate.value);
                return new IndexSupressionPredicate(p);
            } else {
                return predicate;
            }
        }

        @Override
        public Predicate visit(NotEqualPredicate predicate) {
            if (predicate.attributeName.startsWith("%-")) {
                NotEqualPredicate p = new NotEqualPredicate(predicate.attributeName.substring(2), predicate.value);
                return new IndexSupressionPredicate(p);
            } else {
                return predicate;
            }
        }

        @Override
        public Predicate visit(GreaterLessPredicate predicate) {
            if (predicate.attributeName.startsWith("%-")) {
                GreaterLessPredicate p = new GreaterLessPredicate(
                        predicate.attributeName.substring(2), predicate.value, predicate.equal, predicate.less);
                return new IndexSupressionPredicate(p);
            } else {
                return predicate;
            }
        }

        @Override
        public Predicate visit(InPredicate predicate) {
            if (predicate.attributeName.startsWith("%-")) {
                InPredicate p = new InPredicate(predicate.attributeName.substring(2), predicate.values);
                return new IndexSupressionPredicate(p);
            } else {
                return predicate;
            }
        }

        @Override
        public Predicate visitDefault(VisitablePredicate p) {
            return p;
        }
    }
}
