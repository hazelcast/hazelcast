/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.query.impl.predicate.InstanceOfPredicate;

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

    public static Predicate and(Predicate predicate1, Predicate predicate2, Predicate... predicates) {
        return new com.hazelcast.query.impl.predicate.AndPredicate(predicate1, predicate2, predicates);
    }

    public static Predicate not(Predicate predicate) {
        return new com.hazelcast.query.impl.predicate.NotPredicate(predicate);
    }

    /**
     * Or predicate
     *
     * @param predicates
     * @return
     */
    public static Predicate or(Predicate predicate1, Predicate predicate2, Predicate... predicates) {
        return new com.hazelcast.query.impl.predicate.OrPredicate(predicate1, predicate2, predicates);
    }

    public static Predicate notEqual(String attribute, Comparable y) {
        return new com.hazelcast.query.impl.predicate.NotEqualPredicate(attribute, y);
    }

    public static Predicate equal(String attribute, Comparable y) {
        return new com.hazelcast.query.impl.predicate.EqualPredicate(attribute, y);
    }

    public static Predicate like(String attribute, String pattern) {
        return new com.hazelcast.query.impl.predicate.LikePredicate(attribute, pattern);
    }

    public static Predicate ilike(String attribute, String pattern) {
        return new com.hazelcast.query.impl.predicate.ILikePredicate(attribute, pattern);
    }

    public static Predicate regex(String attribute, String pattern) {
        return new com.hazelcast.query.impl.predicate.RegexPredicate(attribute, pattern);
    }

    public static Predicate greaterThan(String x, Comparable y) {
        return new com.hazelcast.query.impl.predicate.GreaterLessPredicate(x, y, false, false);
    }

    public static Predicate greaterEqual(String x, Comparable y) {
        return new com.hazelcast.query.impl.predicate.GreaterLessPredicate(x, y, true, false);
    }

    public static Predicate lessThan(String x, Comparable y) {
        return new com.hazelcast.query.impl.predicate.GreaterLessPredicate(x, y, false, true);
    }

    public static Predicate lessEqual(String x, Comparable y) {
        return new com.hazelcast.query.impl.predicate.GreaterLessPredicate(x, y, true, true);
    }

    public static Predicate between(String attribute, Comparable from, Comparable to) {
        return new com.hazelcast.query.impl.predicate.BetweenPredicate(attribute, from, to);
    }

    public static Predicate in(String attribute, Comparable... values) {
        return new com.hazelcast.query.impl.predicate.InPredicate(attribute, values);
    }

    /**
     * @deprecated As of release 3.4, replaced by {@link com.hazelcast.query.impl.predicate.BetweenPredicate}
     */
    @Deprecated
    public static class BetweenPredicate extends com.hazelcast.query.impl.predicate.BetweenPredicate {
        public BetweenPredicate() {
        }

        public BetweenPredicate(String first, Comparable from, Comparable to) {
            super(first, from, to);
        }
    }

    /**
     * @deprecated As of release 3.4, replaced by {@link com.hazelcast.query.impl.predicate.NotPredicate}
     */
    @Deprecated
    public static class NotPredicate extends com.hazelcast.query.impl.predicate.NotPredicate {
        public NotPredicate(Predicate predicate) {
            super(predicate);
        }

        public NotPredicate() {
        }
    }

    /**
     * @deprecated As of release 3.4, replaced by {@link com.hazelcast.query.impl.predicate.InPredicate}
     */
    @Deprecated
    public static class InPredicate extends com.hazelcast.query.impl.predicate.InPredicate {

        public InPredicate() {
        }

        public InPredicate(String attribute, Comparable... values) {
            super(attribute, values);
        }
    }

    /**
     * @deprecated As of release 3.4, replaced by {@link com.hazelcast.query.impl.predicate.RegexPredicate}
     */
    @Deprecated
    public static class RegexPredicate extends com.hazelcast.query.impl.predicate.RegexPredicate {
        public RegexPredicate() {
        }

        public RegexPredicate(String attribute, String regex) {
            super(attribute, regex);
        }
    }

    /**
     * @deprecated As of release 3.4, replaced by {@link com.hazelcast.query.impl.predicate.AndPredicate}
     */
    @Deprecated
    public static class AndPredicate extends com.hazelcast.query.impl.predicate.AndPredicate {
        public AndPredicate() {
        }

        public AndPredicate(Predicate predicate1, Predicate predicate2, Predicate... predicates) {
            super(predicate1, predicate2, predicates);
        }

        @Deprecated
        public AndPredicate(Predicate... predicates) {
            if (predicates.length < 2) {
                throw new IllegalArgumentException("And predicate takes at least 2 predicate.");
            }
            this.predicates = predicates;
        }
    }

    /**
     * @deprecated As of release 3.4, replaced by {@link com.hazelcast.query.impl.predicate.EqualPredicate}
     */
    @Deprecated
    public static class EqualPredicate extends com.hazelcast.query.impl.predicate.EqualPredicate {
        public EqualPredicate() {
        }

        public EqualPredicate(String attribute) {
            super(attribute);
        }

        public EqualPredicate(String attribute, Comparable value) {
            super(attribute, value);
        }
    }

    /**
     * @deprecated As of release 3.4, replaced by {@link com.hazelcast.query.impl.predicate.GreaterLessPredicate}
     */
    @Deprecated
    public static class GreaterLessPredicate extends com.hazelcast.query.impl.predicate.GreaterLessPredicate {

        public GreaterLessPredicate() {
        }

        public GreaterLessPredicate(String attribute, Comparable value, boolean equal, boolean less) {
            super(attribute, value, equal, less);
        }
    }

    /**
     * @deprecated As of release 3.4, replaced by {@link com.hazelcast.query.impl.predicate.ILikePredicate}
     */
    @Deprecated
    public static class ILikePredicate extends com.hazelcast.query.impl.predicate.ILikePredicate {
        public ILikePredicate() {
        }

        public ILikePredicate(String attribute, String second) {
            super(attribute, second);
        }
    }

    /**
     * @deprecated As of release 3.4, replaced by {@link com.hazelcast.query.impl.predicate.LikePredicate}
     */
    @Deprecated
    public static class LikePredicate extends com.hazelcast.query.impl.predicate.LikePredicate {
        public LikePredicate() {
        }

        public LikePredicate(String attribute, String second) {
            super(attribute, second);
        }
    }

    /**
     * @deprecated As of release 3.4, replaced by {@link com.hazelcast.query.impl.predicate.NotEqualPredicate}
     */
    @Deprecated
    public static class NotEqualPredicate extends com.hazelcast.query.impl.predicate.NotEqualPredicate {

        public NotEqualPredicate() {
        }

        public NotEqualPredicate(String attribute, Comparable value) {
            super(attribute, value);
        }
    }

    /**
     * @deprecated As of release 3.4, replaced by {@link com.hazelcast.query.impl.predicate.OrPredicate}
     */
    @Deprecated
    public static class OrPredicate extends com.hazelcast.query.impl.predicate.OrPredicate {
        public OrPredicate() {
        }

        public OrPredicate(Predicate predicate1, Predicate predicate2, Predicate... predicates) {
            super(predicate1, predicate2, predicates);
        }

        @Deprecated
        public OrPredicate(Predicate... predicates) {
            if (predicates.length < 2) {
                throw new IllegalArgumentException("And predicate takes at least 2 predicate.");
            }
            this.predicates = predicates;
        }
    }


}
