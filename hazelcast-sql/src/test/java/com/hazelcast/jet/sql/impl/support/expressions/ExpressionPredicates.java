/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.support.expressions;

import java.util.function.Predicate;

import static org.junit.Assert.assertNotNull;

@SuppressWarnings({"rawtypes", "unchecked"})
public final class ExpressionPredicates {
    private ExpressionPredicates() {
        // No-op.
    }

    public static Predicate<ExpressionValue> eq(Object operand) {
        return new ComparePredicate(Comparison.EQ, operand, true);
    }

    public static Predicate<ExpressionValue> eq_2(Object operand) {
        return new ComparePredicate(Comparison.EQ, operand, false);
    }

    public static Predicate<ExpressionValue> neq(Object operand) {
        return new ComparePredicate(Comparison.NEQ, operand, true);
    }

    public static Predicate<ExpressionValue> neq_2(Object operand) {
        return new ComparePredicate(Comparison.NEQ, operand, false);
    }

    public static Predicate<ExpressionValue> gt(Object operand) {
        return new ComparePredicate(Comparison.GT, operand, true);
    }

    public static Predicate<ExpressionValue> gt_2(Object operand) {
        return new ComparePredicate(Comparison.GT, operand, false);
    }

    public static Predicate<ExpressionValue> gte(Object operand) {
        return new ComparePredicate(Comparison.GTE, operand, true);
    }

    public static Predicate<ExpressionValue> gte_2(Object operand) {
        return new ComparePredicate(Comparison.GTE, operand, false);
    }

    public static Predicate<ExpressionValue> lt(Object operand) {
        return new ComparePredicate(Comparison.LT, operand, true);
    }

    public static Predicate<ExpressionValue> lt_2(Object operand) {
        return new ComparePredicate(Comparison.LT, operand, false);
    }

    public static Predicate<ExpressionValue> lte(Object operand) {
        return new ComparePredicate(Comparison.LTE, operand, true);
    }

    public static Predicate<ExpressionValue> lte_2(Object operand) {
        return new ComparePredicate(Comparison.LTE, operand, false);
    }

    public static Predicate<ExpressionValue> isNull() {
        return new NullPredicate(true);
    }

    public static Predicate<ExpressionValue> isNull_2() {
        return new NullPredicate(false);
    }

    public static Predicate<ExpressionValue> isNotNull() {
        return new NotNullPredicate(true);
    }

    public static Predicate<ExpressionValue> isNotNull_2() {
        return new NotNullPredicate(false);
    }

    public static Predicate<ExpressionValue> or(Predicate<ExpressionValue>... predicates) {
        return new OrPredicate(predicates);
    }

    public static Predicate<ExpressionValue> and(Predicate<ExpressionValue>... predicates) {
        return new AndPredicate(predicates);
    }

    private enum Comparison {
        EQ, NEQ, GT, GTE, LT, LTE
    }

    public static class ComparePredicate implements Predicate<ExpressionValue> {

        private final Comparison comparison;
        private final Object operand;
        private final boolean firstField;

        public ComparePredicate(Comparison comparison, Object operand, boolean firstField) {
            assertNotNull("Use IsNullPredicate to compare with null", operand);

            this.comparison = comparison;
            this.operand = operand;
            this.firstField = firstField;
        }

        @Override
        public boolean test(ExpressionValue value) {
            Object field = firstField ? value.field1() : ((ExpressionBiValue) value).field2();

            if (field == null) {
                return false;
            }

            int res = ((Comparable) field).compareTo(operand);

            switch (comparison) {
                case GT:
                    return res > 0;

                case GTE:
                    return res >= 0;

                case LT:
                    return res < 0;

                case LTE:
                    return res <= 0;

                case NEQ:
                    return res != 0;

                default:
                    return res == 0;
            }
        }
    }

    public static class NullPredicate implements Predicate<ExpressionValue> {

        private final boolean firstField;

        public NullPredicate(boolean firstField) {
            this.firstField = firstField;
        }

        @Override
        public boolean test(ExpressionValue value) {
            Object field = firstField ? value.field1() : ((ExpressionBiValue) value).field2();

            return field == null;
        }
    }

    public static class NotNullPredicate implements Predicate<ExpressionValue> {

        private final boolean firstField;

        public NotNullPredicate(boolean firstField) {
            this.firstField = firstField;
        }

        @Override
        public boolean test(ExpressionValue value) {
            Object field = firstField ? value.field1() : ((ExpressionBiValue) value).field2();

            return field != null;
        }
    }

    public static class OrPredicate implements Predicate<ExpressionValue> {

        private final Predicate[] predicates;

        public OrPredicate(Predicate... predicates) {
            this.predicates = predicates;
        }

        @Override
        public boolean test(ExpressionValue value) {
            for (Predicate predicate : predicates) {
                if (predicate.test(value)) {
                    return true;
                }
            }

            return false;
        }
    }

    public static class AndPredicate implements Predicate<ExpressionValue> {

        private final Predicate[] predicates;

        public AndPredicate(Predicate... predicates) {
            this.predicates = predicates;
        }

        @Override
        public boolean test(ExpressionValue value) {
            for (Predicate predicate : predicates) {
                if (!predicate.test(value)) {
                    return false;
                }
            }

            return true;
        }
    }
}
