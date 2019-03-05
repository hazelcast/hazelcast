/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

public final class Numbers {

    private Numbers() {
    }

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
                return compareLongs(lhsNumber.longValue(), rhsNumber.longValue());
            }
        }

        return lhs.compareTo(rhs);
    }

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

    public static boolean equalDoubles(double lhs, double rhs) {
        // exactly as Double.equals does it, see https://github.com/hazelcast/hazelcast/issues/6188
        return Double.doubleToLongBits(lhs) == Double.doubleToLongBits(rhs);
    }

    public static boolean equalFloats(float lhs, float rhs) {
        // exactly as Float.equals does it, see https://github.com/hazelcast/hazelcast/issues/6188
        return Float.floatToIntBits(lhs) == Float.floatToIntBits(rhs);
    }

    public static boolean isDoubleRepresentable(Class clazz) {
        return clazz == Double.class || clazz == Float.class;
    }

    public static boolean isLongRepresentable(Class clazz) {
        return clazz == Long.class || clazz == Integer.class || clazz == Short.class || clazz == Byte.class;
    }

    private static boolean isLongRepresentableExceptLong(Class clazz) {
        return clazz == Integer.class || clazz == Short.class || clazz == Byte.class;
    }

    private static int compareLongs(long lhs, long rhs) {
        return lhs < rhs ? -1 : (lhs == rhs ? 0 : +1);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private static int compareLongWithDouble(long l, double d) {
        if (d > -0x1p53 && d < +0x1p53) {
            // All long values in this range are exactly representable as doubles.
            // 2^53 itself is excluded to ensure the correct ordering of 2^53 + 1
            // long value, 2^53 and 2^53 + 1 are the same value when represented
            // as double and that's the first integer value having this property.
            return Double.compare((double) l, d);
        }

        // Only infinities, NaNs and integer double values are left: starting
        // from 2^52, doubles can represent only integer values with increasing
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
            // the world.
            return -1;
        }

        // All remaining double values are integer and less than 2^63 in
        // magnitude, so we may just cast them to long.
        return compareLongs(l, (long) d);
    }

    /**
     * @see #compareLongWithDouble(long, double)
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private static boolean equalLongAndDouble(long l, double d) {
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

}
