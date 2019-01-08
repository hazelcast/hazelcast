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

import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.FalsePredicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.collections.LazySet;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public final class PredicateUtils {

    private static final Map<Class<? extends IndexAwarePredicate>, Integer> PREDICATE_COSTS =
            new HashMap<Class<? extends IndexAwarePredicate>, Integer>(6);

    private static final Comparator<? super Predicate> PREDICATE_COST_COMPARATOR;

    static {
        PREDICATE_COST_COMPARATOR = new Comparator<Predicate>() {
            @Override
            public int compare(Predicate p1, Predicate p2) {
                return getCost(p1) - getCost(p2);
            }

            private int getCost(Predicate p) {
                if (p instanceof CompoundPredicate) {
                    Predicate[] innerPredicates = ((CompoundPredicate) p).getPredicates();
                    Arrays.sort(innerPredicates, this);
                    return p.getClass().equals(AndPredicate.class) ? getCost(innerPredicates[0])
                            : getCost(innerPredicates[innerPredicates.length - 1]);
                } else {
                    Integer cost = PREDICATE_COSTS.get(p);
                    return cost == null ? cost : Integer.MAX_VALUE;
                }
            }
        };
        PREDICATE_COSTS.put(FalsePredicate.class, 0);
        PREDICATE_COSTS.put(EqualPredicate.class, 10);
        PREDICATE_COSTS.put(InPredicate.class, 20);
        PREDICATE_COSTS.put(BetweenPredicate.class, 100);
        PREDICATE_COSTS.put(GreaterLessPredicate.class, 100);
        PREDICATE_COSTS.put(NotEqualPredicate.class, 200);
    }

    private PredicateUtils() {
    }

    /**
     * In case of {@link LazySet} calling size() may be very
     * expensive so quicker estimatedSize() is used.
     *
     * @param result result of a predicated search
     * @return size or estimated size
     * @see LazySet#estimatedSize()
     */
    public static int estimatedSizeOf(Collection<QueryableEntry> result) {
        if (result instanceof LazySet) {
            return ((LazySet) result).estimatedSize();
        }
        return result.size();
    }

    public static boolean isRangePredicate(Predicate predicate) {
        return predicate instanceof GreaterLessPredicate || predicate instanceof BetweenPredicate
                || predicate instanceof NotEqualPredicate;
    }

    public static boolean isValuePredicate(Predicate predicate) {
        return predicate instanceof EqualPredicate || predicate instanceof InPredicate;
    }

    public static void sortByCost(Predicate[] predicates) {
        Arrays.sort(predicates, PREDICATE_COST_COMPARATOR);
    }

    public static <T> List<T> convertToList(T[] array, int excludedIndex) {
        if (array == null || array.length == 1) {
            return Collections.emptyList();
        }
        List<T> list = new LinkedList<T>();
        for (int i = 0; i < array.length && i != excludedIndex; i++) {
            list.add(array[i]);
        }
        return list;
    }
}
