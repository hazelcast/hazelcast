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
                return equal(lhs.doubleValue(), rhs.doubleValue());
            } else if (isLongRepresentable(rhsClass)) {
                // TODO invent a better method of comparing longs and doubles?
                return lhs.doubleValue() == rhs.doubleValue();
            }
        } else if (isLongRepresentable(lhsClass)) {
            if (isDoubleRepresentable(rhsClass)) {
                // TODO invent a better method of comparing longs and doubles?
                return lhs.doubleValue() == rhs.doubleValue();
            } else if (isLongRepresentable(rhsClass)) {
                return lhs.longValue() == rhs.longValue();
            }
        }

        return lhs.equals(rhs);
    }

    public static boolean equal(double lhs, double rhs) {
        // exactly as Double.equals does it, see https://github.com/hazelcast/hazelcast/issues/6188
        return Double.doubleToLongBits(lhs) == Double.doubleToLongBits(rhs);
    }

    public static boolean equal(float lhs, float rhs) {
        // exactly as Float.equals does it, see https://github.com/hazelcast/hazelcast/issues/6188
        return Float.floatToIntBits(lhs) == Float.floatToIntBits(rhs);
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
                // TODO invent a better method of comparing longs and doubles?
                return Double.compare(lhsNumber.doubleValue(), rhsNumber.doubleValue());
            }
        } else if (isLongRepresentable(lhsClass)) {
            if (isDoubleRepresentable(rhsClass)) {
                // TODO invent a better method of comparing longs and doubles?
                return Double.compare(lhsNumber.doubleValue(), rhsNumber.doubleValue());
            } else if (isLongRepresentable(rhsClass)) {
                return compare(lhsNumber.longValue(), rhsNumber.longValue());
            }
        }

        return lhs.compareTo(rhs);
    }

    public static Comparable canonicalize(Comparable value) {
        Class clazz = value.getClass();
        assert value instanceof Number;

        Number number = (Number) value;

        if (isDoubleRepresentable(clazz)) {
            double doubleValue = number.doubleValue();
            long longValue = number.longValue();

            if (equal(doubleValue, (double) longValue)) {
                return longValue;
            } else if (clazz == Float.class) {
                return doubleValue;
            }
        } else if (isLongRepresentableExceptLong(clazz)) {
            return number.longValue();
        }

        return value;
    }

    private static boolean isDoubleRepresentable(Class clazz) {
        return clazz == Double.class || clazz == Float.class;
    }

    private static boolean isLongRepresentable(Class clazz) {
        return clazz == Long.class || clazz == Integer.class || clazz == Short.class || clazz == Byte.class;
    }

    private static boolean isLongRepresentableExceptLong(Class clazz) {
        return clazz == Integer.class || clazz == Short.class || clazz == Byte.class;
    }

    private static int compare(long lhs, long rhs) {
        return lhs < rhs ? -1 : (lhs == rhs ? 0 : +1);
    }

}
