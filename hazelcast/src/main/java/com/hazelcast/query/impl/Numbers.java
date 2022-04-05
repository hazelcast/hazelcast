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

package com.hazelcast.query.impl;

/**
 * Provides utilities which compare and canonicalize {@link Number} instances.
 */
public final class Numbers {

    private Numbers() {
    }

    /**
     * Checks the provided {@link Number} instances for equality.
     * <p>
     * Special numeric comparison logic is used for {@link Double}, {@link Long},
     * {@link Float}, {@link Integer}, {@link Short} and {@link Byte}: two
     * values are considered equal, if both represent a number of the same
     * magnitude and sign, disregarding the actual underlying types.
     *
     * @param lhs the left-hand side {@link Number}. Can't be {@code null}.
     * @param rhs the right-hand side {@link Number}. Can't be {@code null}.
     * @return {@code true} if the provided numbers are equal, {@code false}
     * otherwise.
     */
    public static boolean equal(Number lhs, Number rhs) {
        Class lhsClass = lhs.getClass();
        Class rhsClass = rhs.getClass();
        assert lhsClass != rhsClass;

        if (isDoubleRepresentable(lhsClass)) {
            if (isDoubleRepresentable(rhsClass)) {
                return equalDoubles(lhs.doubleValue(), rhs.doubleValue());
            } else if (isLongRepresentable(rhsClass)) {
                return equalLongAndDouble(rhs.longValue(), lhs.doubleValue());
            }
        } else if (isLongRepresentable(lhsClass)) {
            if (isDoubleRepresentable(rhsClass)) {
                return equalLongAndDouble(lhs.longValue(), rhs.doubleValue());
            } else if (isLongRepresentable(rhsClass)) {
                return lhs.longValue() == rhs.longValue();
            }
        }

        return lhs.equals(rhs);
    }

    /**
     * Compares the provided {@link Number} instances.
     * <p>
     * The method accepts {@link Comparable} instances because to compare
     * something it must be comparable, but {@link Number} is not comparable.
     * <p>
     * Special numeric comparison logic is used for {@link Double}, {@link Long},
     * {@link Float}, {@link Integer}, {@link Short} and {@link Byte}:
     * comparison is performed by taking into account only the magnitudes and
     * signs of the passed numbers, disregarding the actual underlying types.
     *
     * @param lhs the left-hand side {@link Number}. Can't be {@code null}.
     * @param rhs the right-hand side {@link Number}. Can't be {@code null}.
     * @return a negative integer, zero, or a positive integer as the left-hand
     * side {@link Number} is less than, equal to, or greater than the
     * right-hand side {@link Number}.
     */

    @SuppressWarnings("unchecked")
    public static int compare(Comparable lhs, Comparable rhs) {
        Class lhsClass = lhs.getClass();
        Class rhsClass = rhs.getClass();
        assert lhsClass != rhsClass;
        assert lhs instanceof Number;
        assert rhs instanceof Number;

        Number lhsNumber = (Number) lhs;
        Number rhsNumber = (Number) rhs;

        if (isDoubleRepresentable(lhsClass)) {
            if (isDoubleRepresentable(rhsClass)) {
                return Double.compare(lhsNumber.doubleValue(), rhsNumber.doubleValue());
            } else if (isLongRepresentable(rhsClass)) {
                return -Integer.signum(compareLongWithDouble(rhsNumber.longValue(), lhsNumber.doubleValue()));
            }
        } else if (isLongRepresentable(lhsClass)) {
            if (isDoubleRepresentable(rhsClass)) {
                return compareLongWithDouble(lhsNumber.longValue(), rhsNumber.doubleValue());
            } else if (isLongRepresentable(rhsClass)) {
                return Long.compare(lhsNumber.longValue(), rhsNumber.longValue());
            }
        }

        return lhs.compareTo(rhs);
    }

    /**
     * Canonicalizes the given {@link Number} value for the purpose of a
     * hash-based lookup.
     * <p>
     * The method accepts a {@link Comparable} instance and returns one because
     * query engine acts on comparable values, but {@link Number} is not
     * comparable.
     * <p>
     * Special numeric canonicalization logic is used for {@link Double},
     * {@link Long}, {@link Float}, {@link Integer}, {@link Short} and
     * {@link Byte}: all whole numbers in the [{@link Long#MIN_VALUE},
     * {@link Long#MAX_VALUE}] range are represented as {@link Long}, all other
     * whole and non-whole numbers are presented as {@link Double}. That logic
     * allows to mix numeric types while performing hash-based lookups, e.g.
     * while using one of the standard {@link java.util.Map} implementations.
     *
     * @param value the {@link Number} to canonicalize.
     * @return a canonical representation of the given {@link Number} or
     * the original {@link Number} if there is no special canonical
     * representation for it.
     */
    public static Comparable canonicalizeForHashLookup(Comparable value) {
        Class clazz = value.getClass();
        assert value instanceof Number;

        Number number = (Number) value;

        if (isDoubleRepresentable(clazz)) {
            double doubleValue = number.doubleValue();
            long longValue = number.longValue();

            if (equalDoubles(doubleValue, (double) longValue)) {
                return longValue;
            } else if (clazz == Float.class) {
                return doubleValue;
            }
        } else if (isLongRepresentableExceptLong(clazz)) {
            return number.longValue();
        }

        return value;
    }

    /**
     * Checks the provided double values for equality exactly as {@link
     * Double#equals} does it, avoiding the boxing-unboxing cycle.
     *
     * @param lhs the left-hand side double value.
     * @param rhs the right-hand side double value.
     * @return {@code true} if the given values are equal, {@code false}
     * otherwise.
     */
    public static boolean equalDoubles(double lhs, double rhs) {
        // see https://github.com/hazelcast/hazelcast/issues/6188
        return Double.doubleToLongBits(lhs) == Double.doubleToLongBits(rhs);
    }

    /**
     * Checks the provided float values for equality exactly as {@link
     * Float#equals} does it, avoiding the boxing-unboxing cycle.
     *
     * @param lhs the left-hand side float value.
     * @param rhs the right-hand side float value.
     * @return {@code true} if the given values are equal, {@code false}
     * otherwise.
     */
    public static boolean equalFloats(float lhs, float rhs) {
        // see https://github.com/hazelcast/hazelcast/issues/6188
        return Float.floatToIntBits(lhs) == Float.floatToIntBits(rhs);
    }

    /**
     * Represents the given {@link Number} exactly as a double value without any
     * magnitude and precision losses; if that's not possible, fails by throwing
     * an exception.
     *
     * @param number the number to represent as a double value.
     * @return a double representation of the given number.
     * @throws IllegalArgumentException if no exact representation exists.
     */
    public static double asDoubleExactly(Number number) {
        Class clazz = number.getClass();

        if (isDoubleRepresentable(clazz) || isLongRepresentableExceptLong(clazz)) {
            return number.doubleValue();
        } else if (clazz == Long.class) {
            double doubleValue = number.doubleValue();
            if (number.longValue() == (long) doubleValue) {
                return doubleValue;
            }
        }

        throw new IllegalArgumentException("Can't represent " + number + " as double exactly");
    }

    /**
     * Represents the given {@link Number} exactly as a long value without any
     * magnitude and precision losses; if that's not possible, fails by throwing
     * an exception.
     *
     * @param number the number to represent as a long value.
     * @return a long representation of the given number.
     * @throws IllegalArgumentException if no exact representation exists.
     */
    public static long asLongExactly(Number number) {
        Class clazz = number.getClass();

        if (isLongRepresentable(clazz)) {
            return number.longValue();
        } else if (isDoubleRepresentable(clazz)) {
            long longValue = number.longValue();
            if (equalDoubles(number.doubleValue(), (double) longValue)) {
                return longValue;
            }
        }

        throw new IllegalArgumentException("Can't represent " + number + " as long exactly");
    }

    /**
     * Represents the given {@link Number} exactly as an int value without any
     * magnitude and precision losses; if that's not possible, fails by throwing
     * an exception.
     *
     * @param number the number to represent as an int value.
     * @return an int representation of the given number.
     * @throws IllegalArgumentException if no exact representation exists.
     */
    @SuppressWarnings("RedundantCast")
    public static int asIntExactly(Number number) {
        Class clazz = number.getClass();

        if (isLongRepresentableExceptLong(clazz)) {
            return number.intValue();
        } else if (clazz == Long.class) {
            int intValue = number.intValue();
            if (number.longValue() == (long) intValue) {
                return intValue;
            }
        } else if (isDoubleRepresentable(clazz)) {
            int intValue = number.intValue();
            if (equalDoubles(number.doubleValue(), (double) intValue)) {
                return intValue;
            }
        }

        throw new IllegalArgumentException("Can't represent " + number + " as int exactly");
    }

    /**
     * @return {@code true} if instances of the given class can be represented
     * as double values without any magnitude and precision losses, {@code false}
     * otherwise.
     */
    public static boolean isDoubleRepresentable(Class clazz) {
        return clazz == Double.class || clazz == Float.class;
    }

    /**
     * @return {@code true} if instances of the given class can be represented
     * as long values without any magnitude and precision losses, {@code false}
     * otherwise.
     */
    public static boolean isLongRepresentable(Class clazz) {
        return clazz == Long.class || clazz == Integer.class || clazz == Short.class || clazz == Byte.class;
    }

    /**
     * Checks the provided long and double values for equality.
     * <p>
     * The method fully avoids magnitude and precision losses while performing
     * the equality check, e.g. {@code (double) Long.MAX_VALUE} is not equal to
     * {@code Long.MAX_VALUE - 1}.
     *
     * @param l the long value to check the equality of.
     * @param d the double value to check the equality of.
     * @return {@code true} if the provided values are equal, {@code false}
     * otherwise.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static boolean equalLongAndDouble(long l, double d) {
        // see compareLongWithDouble for more details on what is going on here

        if (d > -0x1p53 && d < +0x1p53) {
            return equalDoubles((double) l, d);
        }

        if (d <= -0x1p63) {
            return false;
        }

        if (d >= +0x1p63) {
            return false;
        }

        if (Double.isNaN(d)) {
            return false;
        }

        return l == (long) d;
    }

    private static boolean isLongRepresentableExceptLong(Class clazz) {
        return clazz == Integer.class || clazz == Short.class || clazz == Byte.class;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private static int compareLongWithDouble(long l, double d) {
        if (d > -0x1p53 && d < +0x1p53) {
            // Whole numbers in this range are exactly representable as doubles.
            // After casting the given long value to a double, it either falls
            // in this same range and thus it's safe to compare it with the
            // given double value; or it falls outside of the range and this
            // indicates that the long value is less or greater than the given
            // double value and it's also safe to compare it since we already
            // limited the double range to exactly representable whole numbers.
            //
            // 2^53 itself is excluded to ensure the correct ordering of 2^53 + 1:
            // 2^53 and 2^53 + 1 are the same value when represented as a double
            // and that's the first whole number having this property.
            //
            // Double.compare also handles the correct ordering of negative and
            // positive zeroes for us.
            return Double.compare((double) l, d);
        }

        // Only infinities, NaNs and whole double values are left: starting
        // from 2^52, doubles can represent only whole values with increasing
        // gaps between them.

        if (d <= -0x1p63) {
            //  -92233720368547_76000 (0x1p63) < -92233720368547_75808 (-2^63 = Long.MIN_VALUE)
            // the next representable double value is -92233720368547_74800.
            return +1;
        }

        if (d >= +0x1p63) {
            // 92233720368547_75807 (2^63 - 1 = Long.MAX_VALUE) < 92233720368547_76000 (0x1p63)
            // the previous representable double value is 92233720368547_74800.
            return -1;
        }

        // Infinities are gone at this point.

        if (Double.isNaN(d)) {
            // NaNs are ordered by Double.compareTo as the biggest numbers in
            // the world. At the same time, they are neither equal to, less or
            // greater than any double value (including NaNs themselves) when
            // compared using the Java comparison operators.
            return -1;
        }

        // All remaining double values are whole numbers and less than 2^63 in
        // magnitude, so we may just cast them to long.
        return Long.compare(l, (long) d);
    }

}
