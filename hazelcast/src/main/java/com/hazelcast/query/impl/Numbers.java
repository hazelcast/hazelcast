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

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:methodlength"})
    public static boolean equals(Number lhs, Number rhs) {
        Class lhsClass = lhs.getClass();
        Class rhsClass = rhs.getClass();
        assert lhsClass != rhsClass;

        if (isDoubleRepresentable(lhsClass)) {
            if (isDoubleRepresentable(rhsClass)) {
                // exactly as Double.equals does it, see https://github.com/hazelcast/hazelcast/issues/6188
                return Double.doubleToLongBits(lhs.doubleValue()) == Double.doubleToLongBits(rhs.doubleValue());
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

    @SuppressWarnings({"unchecked", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength", "checkstyle:returncount"})
    public static int compare(Comparable lhs, Comparable rhs) {
        Class lhsClass = lhs.getClass();
        Class rhsClass = rhs.getClass();
        assert lhsClass != rhsClass;
        assert lhs instanceof Number;
        assert rhs instanceof Number;

        Number lhsNumber = (Number) lhs;
        Number rhsNumber = (Number) rhs;

        if (lhsClass == Long.class) {
            if (rhsClass == Double.class) {
                return Double.compare(lhsNumber.doubleValue(), rhsNumber.doubleValue());
            } else if (rhsClass == Integer.class) {
                return compare(lhsNumber.longValue(), rhsNumber.longValue());
            } else if (rhsClass == Float.class) {
                return Float.compare(lhsNumber.floatValue(), rhsNumber.floatValue());
            } else if (rhsClass == Short.class) {
                return compare(lhsNumber.longValue(), rhsNumber.longValue());
            } else if (rhsClass == Byte.class) {
                return compare(lhsNumber.longValue(), rhsNumber.longValue());
            }
        } else if (lhsClass == Double.class) {
            if (rhsClass == Long.class) {
                return Double.compare(lhsNumber.doubleValue(), rhsNumber.doubleValue());
            } else if (rhsClass == Integer.class) {
                return Double.compare(lhsNumber.doubleValue(), rhsNumber.doubleValue());
            } else if (rhsClass == Float.class) {
                return Double.compare(lhsNumber.doubleValue(), rhsNumber.doubleValue());
            } else if (rhsClass == Short.class) {
                return Double.compare(lhsNumber.doubleValue(), rhsNumber.doubleValue());
            } else if (rhsClass == Byte.class) {
                return Double.compare(lhsNumber.doubleValue(), rhsNumber.doubleValue());
            }
        } else if (lhsClass == Integer.class) {
            if (rhsClass == Long.class) {
                return compare(lhsNumber.longValue(), rhsNumber.longValue());
            } else if (rhsClass == Double.class) {
                return Double.compare(lhsNumber.doubleValue(), rhsNumber.doubleValue());
            } else if (rhsClass == Float.class) {
                return Float.compare(lhsNumber.floatValue(), rhsNumber.floatValue());
            } else if (rhsClass == Short.class) {
                return compare(lhsNumber.intValue(), rhsNumber.intValue());
            } else if (rhsClass == Byte.class) {
                return compare(lhsNumber.intValue(), rhsNumber.intValue());
            }
        } else if (lhsClass == Float.class) {
            if (rhsClass == Long.class) {
                return Float.compare(lhsNumber.floatValue(), rhsNumber.floatValue());
            } else if (rhsClass == Double.class) {
                return Double.compare(lhsNumber.doubleValue(), rhsNumber.doubleValue());
            } else if (rhsClass == Integer.class) {
                return Float.compare(lhsNumber.floatValue(), rhsNumber.floatValue());
            } else if (rhsClass == Short.class) {
                return Float.compare(lhsNumber.floatValue(), rhsNumber.floatValue());
            } else if (rhsClass == Byte.class) {
                return Float.compare(lhsNumber.floatValue(), rhsNumber.floatValue());
            }
        } else if (lhsClass == Short.class) {
            if (rhsClass == Long.class) {
                return compare(lhsNumber.longValue(), rhsNumber.longValue());
            } else if (rhsClass == Double.class) {
                return Double.compare(lhsNumber.doubleValue(), rhsNumber.doubleValue());
            } else if (rhsClass == Integer.class) {
                return compare(lhsNumber.intValue(), rhsNumber.intValue());
            } else if (rhsClass == Float.class) {
                return Float.compare(lhsNumber.floatValue(), rhsNumber.floatValue());
            } else if (rhsClass == Byte.class) {
                return compare(lhsNumber.shortValue(), rhsNumber.shortValue());
            }
        } else if (lhsClass == Byte.class) {
            if (rhsClass == Long.class) {
                return compare(lhsNumber.longValue(), rhsNumber.longValue());
            } else if (rhsClass == Double.class) {
                return Double.compare(lhsNumber.doubleValue(), rhsNumber.doubleValue());
            } else if (rhsClass == Integer.class) {
                return compare(lhsNumber.intValue(), rhsNumber.intValue());
            } else if (rhsClass == Float.class) {
                return Float.compare(lhsNumber.floatValue(), rhsNumber.floatValue());
            } else if (rhsClass == Short.class) {
                return compare(lhsNumber.shortValue(), rhsNumber.shortValue());
            }
        }

        return lhs.compareTo(rhs);
    }

    private static int compare(long x, long y) {
        return x < y ? -1 : (x == y ? 0 : 1);
    }

    private static int compare(int x, int y) {
        return x < y ? -1 : (x == y ? 0 : 1);
    }

    private static int compare(short x, short y) {
        return x < y ? -1 : (x == y ? 0 : 1);
    }

    private static boolean isDoubleRepresentable(Class clazz) {
        return clazz == Double.class || clazz == Float.class;
    }

    private static boolean isLongRepresentable(Class clazz) {
        return clazz == Long.class || clazz == Integer.class || clazz == Short.class || clazz == Byte.class;
    }

}
