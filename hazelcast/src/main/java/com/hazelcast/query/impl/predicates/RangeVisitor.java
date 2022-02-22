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

import com.hazelcast.core.TypeConverter;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.Comparables;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.TypeConverters;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.HashMap;

import static com.hazelcast.query.impl.AbstractIndex.NULL;
import static com.hazelcast.query.impl.predicates.PredicateUtils.isNull;

/**
 * Performs range predicates optimization.
 * <p>
 * The basic idea is to reduce all predicates for a certain attribute into a
 * single predicate verifying its satisfiability on the go. For instance:
 * <ul>
 * <li>"a = 1 and a = 2" and "a &gt; 10 and a &lt; 5" are unsatisfiable.
 * <li>"a &gt; 0 and a &lt; 10" and "a &gt; 0 and a &gt; 10" can be reduced into a single
 * range predicate.
 * </ul>
 * {@link CompositeIndexVisitor} is highly dependent on the work of this class
 * and assumes that predicates fed into it are already range-optimized.
 */
public class RangeVisitor extends AbstractVisitor {

    @Override
    public Predicate visit(AndPredicate predicate, Indexes indexes) {
        Predicate[] predicates = predicate.predicates;
        Ranges ranges = null;

        for (int i = 0; i < predicates.length; ++i) {
            ranges = intersect(predicates, i, ranges, indexes);
            if (ranges == Ranges.UNSATISFIABLE) {
                return Predicates.alwaysFalse();
            }
        }

        return ranges == null ? predicate : ranges.generate(predicate);
    }

    @Override
    public Predicate visit(BetweenPredicate predicate, Indexes indexes) {
        TypeConverter converter = indexes.getConverter(predicate.attributeName);
        if (converter == null) {
            return predicate;
        }

        Comparable from = converter.convert(predicate.from);
        Comparable to = converter.convert(predicate.to);
        Order order = compare(from, to);
        switch (order) {
            case LESS:
                return predicate;
            case EQUAL:
                return Predicates.equal(predicate.attributeName, from);
            case GREATER:
                return Predicates.alwaysFalse();

            default:
                throw new IllegalStateException("Unexpected order: " + order);
        }
    }

    private static Ranges intersect(Predicate[] predicates, int predicateIndex, Ranges ranges, Indexes indexes) {
        Predicate predicate = predicates[predicateIndex];

        if (predicate instanceof FalsePredicate) {
            return Ranges.UNSATISFIABLE;
        } else if (!isSupportedPredicate(predicate)) {
            return ranges;
        }

        RangePredicate rangePredicate = (RangePredicate) predicate;
        String attribute = rangePredicate.getAttribute();
        Range existingRange = Ranges.getRange(attribute, ranges);
        Range range = intersect(rangePredicate, existingRange, indexes);

        if (range == Range.UNKNOWN) {
            return ranges;
        } else if (range == Range.UNSATISFIABLE) {
            return Ranges.UNSATISFIABLE;
        }

        if (ranges == null) {
            ranges = new Ranges(predicates.length);
        }
        ranges.addRange(attribute, range, existingRange, predicateIndex);

        return ranges;
    }

    private static boolean isSupportedPredicate(Predicate predicate) {
        if (!(predicate instanceof RangePredicate)) {
            return false;
        }

        RangePredicate rangePredicate = (RangePredicate) predicate;
        // we are unable to reason about multi-value attributes currently
        return !rangePredicate.getAttribute().contains("[any]");
    }

    private static Range intersect(RangePredicate predicate, Range range, Indexes indexes) {
        if (range == null) {
            TypeConverter converter = indexes.getConverter(predicate.getAttribute());
            if (converter == null) {
                return Range.UNKNOWN;
            }

            return new Range(predicate, converter);
        } else {
            return range.intersect(predicate);
        }
    }

    private static Order compare(Comparable lhs, Comparable rhs) {
        int order = Comparables.compare(lhs, rhs);
        if (order < 0) {
            return Order.LESS;
        } else if (order == 0) {
            return Order.EQUAL;
        } else {
            return Order.GREATER;
        }
    }

    private enum Order {
        LESS,
        EQUAL,
        GREATER
    }

    @SuppressFBWarnings("SE_BAD_FIELD")
    private static class Ranges extends HashMap<String, Range> {

        public static final Ranges UNSATISFIABLE = new Ranges();

        private final Range[] rangesByPredicateIndex;

        private int reduction;

        Ranges(int predicateCount) {
            super(predicateCount);
            this.rangesByPredicateIndex = new Range[predicateCount];
        }

        private Ranges() {
            this.rangesByPredicateIndex = null;
        }

        public static Range getRange(String attribute, Ranges ranges) {
            return ranges == null ? null : ranges.getRange(attribute);
        }

        public Range getRange(String attribute) {
            assert rangesByPredicateIndex != null;
            return get(attribute);
        }

        public void addRange(String attribute, Range range, Range existingRange, int predicateIndex) {
            assert rangesByPredicateIndex != null;

            put(attribute, range);
            rangesByPredicateIndex[predicateIndex] = range;
            if (existingRange != null) {
                ++reduction;
            }
        }

        public Predicate generate(AndPredicate originalAndPredicate) {
            assert rangesByPredicateIndex != null;

            if (reduction == 0) {
                return originalAndPredicate;
            }
            Predicate[] originalPredicates = originalAndPredicate.predicates;

            int predicateCount = originalPredicates.length - reduction;
            assert predicateCount > 0;
            Predicate[] predicates = new Predicate[predicateCount];

            int generated = 0;
            for (int i = 0; i < originalPredicates.length; ++i) {
                Range range = rangesByPredicateIndex[i];
                if (range == null) {
                    predicates[generated++] = originalPredicates[i];
                } else {
                    Predicate predicate = range.generate(originalPredicates[i]);
                    if (predicate != null) {
                        predicates[generated++] = predicate;
                    }
                }
            }
            assert generated == predicateCount;

            return predicateCount == 1 ? predicates[0] : new AndPredicate(predicates);
        }

    }

    private static class Range {

        /**
         * Indicates a range with unknown satisfiability for which there is no
         * enough type information available to decide on its satisfiability or
         * unsatisfiability.
         */
        public static final Range UNKNOWN = new Range();

        /**
         * Indicates an unsatisfiable range like {@code a < 100 and a > 100}.
         */
        public static final Range UNSATISFIABLE = new Range();

        private final String attribute;
        private final TypeConverter converter;

        private Comparable from;
        private boolean fromInclusive;

        private Comparable to;
        private boolean toInclusive;

        private boolean intersected;
        private boolean generated;

        Range(RangePredicate predicate, TypeConverter converter) {
            this.attribute = predicate.getAttribute();
            this.converter = converter;

            this.from = convert(predicate.getFrom(), predicate.isFromInclusive());
            this.fromInclusive = predicate.isFromInclusive();
            this.to = convert(predicate.getTo(), predicate.isToInclusive());
            this.toInclusive = predicate.isToInclusive();
            assert isNullnessCheck() || from != NULL && to != NULL;
        }

        private Range() {
            this.attribute = null;
            this.converter = TypeConverters.IDENTITY_CONVERTER;
        }

        @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:methodlength", "checkstyle:npathcomplexity"})
        public Range intersect(RangePredicate predicate) {
            intersected = true;

            Comparable from = convert(predicate.getFrom(), predicate.isFromInclusive());
            boolean fromInclusive = predicate.isFromInclusive();
            Comparable to = convert(predicate.getTo(), predicate.isToInclusive());
            boolean toInclusive = predicate.isToInclusive();

            if (isNull(from) && isNull(to)) {
                assert fromInclusive && toInclusive;
                return isNullnessCheck() ? this : UNSATISFIABLE;
            } else if (isNullnessCheck()) {
                return UNSATISFIABLE;
            }
            assert from != NULL && to != NULL;

            if (this.from == null) {
                this.from = from;
                this.fromInclusive = fromInclusive;
            } else if (from != null) {
                switch (compare(this.from, from)) {
                    case LESS:
                        this.from = from;
                        this.fromInclusive = fromInclusive;
                        break;
                    case EQUAL:
                        this.fromInclusive &= fromInclusive;
                        break;
                    case GREATER:
                        // do nothing
                        break;
                    default:
                        throw new IllegalStateException("unexpected order");
                }
            }

            if (this.to == null) {
                this.to = to;
                this.toInclusive = toInclusive;
            } else if (to != null) {
                switch (compare(this.to, to)) {
                    case LESS:
                        // do nothing
                        break;
                    case EQUAL:
                        this.toInclusive &= toInclusive;
                        break;
                    case GREATER:
                        this.to = to;
                        this.toInclusive = toInclusive;
                        break;
                    default:
                        throw new IllegalStateException("unexpected order");
                }
            }

            if (this.from != null && this.to != null) {
                switch (compare(this.from, this.to)) {
                    case LESS:
                        return this;
                    case EQUAL:
                        return this.fromInclusive && this.toInclusive ? this : UNSATISFIABLE;
                    case GREATER:
                        return UNSATISFIABLE;
                    default:
                        throw new IllegalStateException("unexpected order");
                }
            }

            return this;
        }

        @SuppressWarnings("checkstyle:cyclomaticcomplexity")
        public Predicate generate(Predicate originalPredicate) {
            if (generated) {
                return null;
            }
            generated = true;

            if (!intersected) {
                return originalPredicate;
            }

            if (isNullnessCheck()) {
                return new EqualPredicate(attribute, NULL);
            }
            assert from != NULL && to != NULL;

            if (from == null) {
                return new GreaterLessPredicate(attribute, to, toInclusive, true);
            } else if (to == null) {
                return new GreaterLessPredicate(attribute, from, fromInclusive, false);
            } else if (from == to || Comparables.compare(from, to) == 0) {
                // If from equals to, the predicate may be satisfiable only if
                // both bounds are inclusive.
                assert fromInclusive && toInclusive;
                return new EqualPredicate(attribute, from);
            } else if (fromInclusive && toInclusive) {
                return new BetweenPredicate(attribute, from, to);
            } else {
                return new BoundedRangePredicate(attribute, from, fromInclusive, to, toInclusive);
            }
        }

        private Comparable convert(Comparable value, boolean convertNull) {
            if (value == null) {
                return convertNull ? converter.convert(null) : null;
            } else {
                return converter.convert(value);
            }
        }

        /**
         * @return {@code true} if this range is a nullness check range like
         * {@code a = null}, {@code false} otherwise.
         */
        private boolean isNullnessCheck() {
            if (isNull(from) && isNull(to)) {
                assert fromInclusive && toInclusive;
                return true;
            } else {
                return false;
            }
        }

    }

}
