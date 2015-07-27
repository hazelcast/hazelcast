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

package com.hazelcast.query;

import com.hazelcast.query.impl.predicates.AndPredicate;
import com.hazelcast.query.impl.predicates.BetweenPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.query.impl.predicates.GreaterLessPredicate;
import com.hazelcast.query.impl.predicates.ILikePredicate;
import com.hazelcast.query.impl.predicates.InPredicate;
import com.hazelcast.query.impl.predicates.InstanceOfPredicate;
import com.hazelcast.query.impl.predicates.LikePredicate;
import com.hazelcast.query.impl.predicates.NotEqualPredicate;
import com.hazelcast.query.impl.predicates.NotPredicate;
import com.hazelcast.query.impl.predicates.OrPredicate;
import com.hazelcast.query.impl.predicates.RegexPredicate;

/**
 * A utility class to create {@link com.hazelcast.query.Predicate} instances.
 */
public final class Predicates {

    //we don't want instances. private constructor.
    private Predicates() {
    }

    public static Predicate instanceOf(final Class klass) {
        return new InstanceOfPredicate(klass);
    }

    public static Predicate and(Predicate... predicates) {
        return new AndPredicate(predicates);
    }

    public static Predicate not(Predicate predicate) {
        return new NotPredicate(predicate);
    }

    /**
     * Or predicate
     *
     * @param predicates
     * @return
     */
    public static Predicate or(Predicate... predicates) {
        return new OrPredicate(predicates);
    }

    public static Predicate notEqual(String attribute, Comparable y) {
        return new NotEqualPredicate(attribute, y);
    }

    public static Predicate equal(String attribute, Comparable y) {
        return new EqualPredicate(attribute, y);
    }

    public static Predicate like(String attribute, String pattern) {
        return new LikePredicate(attribute, pattern);
    }

    public static Predicate ilike(String attribute, String pattern) {
        return new ILikePredicate(attribute, pattern);
    }

    public static Predicate regex(String attribute, String pattern) {
        return new RegexPredicate(attribute, pattern);
    }

    public static Predicate greaterThan(String x, Comparable y) {
        return new GreaterLessPredicate(x, y, false, false);
    }

    public static Predicate greaterEqual(String x, Comparable y) {
        return new GreaterLessPredicate(x, y, true, false);
    }

    public static Predicate lessThan(String x, Comparable y) {
        return new GreaterLessPredicate(x, y, false, true);
    }

    public static Predicate lessEqual(String x, Comparable y) {
        return new GreaterLessPredicate(x, y, true, true);
    }

    public static Predicate between(String attribute, Comparable from, Comparable to) {
        return new BetweenPredicate(attribute, from, to);
    }

    public static Predicate in(String attribute, Comparable... values) {
        return new InPredicate(attribute, values);
    }

}
