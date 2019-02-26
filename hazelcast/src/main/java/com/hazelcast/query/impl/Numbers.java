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

import java.math.BigDecimal;
import java.math.BigInteger;

public final class Numbers {

    private Numbers() {
    }

    public static boolean equals(Number lhs, Number rhs) {
        Class lhsClass = lhs.getClass();
        Class rhsClass = rhs.getClass();
        assert lhsClass != rhsClass;

        if (lhsClass == Long.class) {
            if (rhsClass == Double.class) {
                return lhs.doubleValue() == rhs.doubleValue();
            } else if (rhsClass == Integer.class) {
                return lhs.longValue() == rhs.longValue();
            } else if (rhsClass == Float.class) {
                return lhs.floatValue() == rhs.floatValue();
            } else if (rhsClass == Short.class) {
                return lhs.longValue() == rhs.longValue();
            } else if (rhsClass == Byte.class) {
                return lhs.longValue() == rhs.longValue();
            } else if (rhs instanceof BigInteger) {
                return BigInteger.valueOf(lhs.longValue()).equals(rhs);
            } else if (rhs instanceof BigDecimal) {
                return BigDecimal.valueOf(lhs.longValue()).equals(rhs);
            }
        } else if (lhsClass == Double.class) {
            if (rhsClass == Long.class) {
                return lhs.doubleValue() == rhs.doubleValue();
            } else if (rhsClass == Integer.class) {
                return lhs.doubleValue() == rhs.doubleValue();
            } else if (rhsClass == Float.class) {
                return lhs.doubleValue() == rhs.doubleValue();
            } else if (rhsClass == Short.class) {
                return lhs.doubleValue() == rhs.doubleValue();
            } else if (rhsClass == Byte.class) {
                return lhs.doubleValue() == rhs.doubleValue();
            } else if (rhs instanceof BigInteger) {
                return new BigDecimal(lhs.doubleValue()).equals(new BigDecimal((BigInteger) rhs));
            } else if (rhs instanceof BigDecimal) {
                return new BigDecimal(lhs.doubleValue()).equals(rhs);
            }
        } else if (lhsClass == Integer.class) {
            if (rhsClass == Long.class) {
                return lhs.longValue() == rhs.longValue();
            } else if (rhsClass == Double.class) {
                return lhs.doubleValue() == rhs.doubleValue();
            } else if (rhsClass == Float.class) {
                return lhs.floatValue() == rhs.floatValue();
            } else if (rhsClass == Short.class) {
                return lhs.intValue() == rhs.intValue();
            } else if (rhsClass == Byte.class) {
                return lhs.intValue() == rhs.intValue();
            } else if (rhs instanceof BigInteger) {
                return BigInteger.valueOf(lhs.intValue()).equals(rhs);
            } else if (rhs instanceof BigDecimal) {
                return BigDecimal.valueOf(lhs.intValue()).equals(rhs);
            }
        } else if (lhsClass == Float.class) {
            if (rhsClass == Long.class) {
                return lhs.floatValue() == rhs.floatValue();
            } else if (rhsClass == Double.class) {
                return lhs.doubleValue() == rhs.doubleValue();
            } else if (rhsClass == Integer.class) {
                return lhs.floatValue() == rhs.floatValue();
            } else if (rhsClass == Short.class) {
                return lhs.floatValue() == rhs.floatValue();
            } else if (rhsClass == Byte.class) {
                return lhs.floatValue() == rhs.floatValue();
            } else if (rhs instanceof BigInteger) {
                return new BigDecimal(lhs.floatValue()).equals(new BigDecimal((BigInteger) rhs));
            } else if (rhs instanceof BigDecimal) {
                return new BigDecimal(lhs.floatValue()).equals(rhs);
            }
        } else if (lhsClass == Short.class) {
            if (rhsClass == Long.class) {
                return lhs.longValue() == rhs.longValue();
            } else if (rhsClass == Double.class) {
                return lhs.doubleValue() == rhs.doubleValue();
            } else if (rhsClass == Integer.class) {
                return lhs.intValue() == rhs.intValue();
            } else if (rhsClass == Float.class) {
                return lhs.floatValue() == rhs.floatValue();
            } else if (rhsClass == Byte.class) {
                return lhs.shortValue() == rhs.shortValue();
            } else if (rhs instanceof BigInteger) {
                return BigInteger.valueOf(lhs.shortValue()).equals(rhs);
            } else if (rhs instanceof BigDecimal) {
                return BigDecimal.valueOf(lhs.shortValue()).equals(rhs);
            }
        } else if (lhsClass == Byte.class) {
            if (rhsClass == Long.class) {
                return lhs.longValue() == rhs.longValue();
            } else if (rhsClass == Double.class) {
                return lhs.doubleValue() == rhs.doubleValue();
            } else if (rhsClass == Integer.class) {
                return lhs.intValue() == rhs.intValue();
            } else if (rhsClass == Float.class) {
                return lhs.floatValue() == rhs.floatValue();
            } else if (rhsClass == Short.class) {
                return lhs.shortValue() == rhs.shortValue();
            } else if (rhs instanceof BigInteger) {
                return BigInteger.valueOf(lhs.shortValue()).equals(rhs);
            } else if (rhs instanceof BigDecimal) {
                return BigDecimal.valueOf(lhs.shortValue()).equals(rhs);
            }
        } else if (lhs instanceof BigInteger) {
            if (rhsClass == Long.class) {
                return lhs.equals(BigInteger.valueOf(rhs.longValue()));
            } else if (rhsClass == Double.class) {
                return new BigDecimal((BigInteger) lhs).equals(new BigDecimal(rhs.doubleValue()));
            } else if (rhsClass == Integer.class) {
                return lhs.equals(BigInteger.valueOf(rhs.intValue()));
            } else if (rhsClass == Float.class) {
                return new BigDecimal((BigInteger) lhs).equals(new BigDecimal(rhs.floatValue()));
            } else if (rhsClass == Short.class) {
                return new BigDecimal((BigInteger) lhs).equals(BigDecimal.valueOf(rhs.shortValue()));
            } else if (rhsClass == Byte.class) {
                return new BigDecimal((BigInteger) lhs).equals(BigDecimal.valueOf(rhs.byteValue()));
            } else if (rhs instanceof BigDecimal) {
                return new BigDecimal((BigInteger) lhs).equals(rhs);
            }
        } else if (lhs instanceof BigDecimal) {
            if (rhsClass == Long.class) {
                return lhs.equals(BigDecimal.valueOf(rhs.longValue()));
            } else if (rhsClass == Double.class) {
                return lhs.equals(new BigDecimal(rhs.doubleValue()));
            } else if (rhsClass == Integer.class) {
                return lhs.equals(BigDecimal.valueOf(rhs.intValue()));
            } else if (rhsClass == Float.class) {
                return lhs.equals(new BigDecimal(rhs.floatValue()));
            } else if (rhsClass == Short.class) {
                return lhs.equals(BigDecimal.valueOf(rhs.shortValue()));
            } else if (rhsClass == Byte.class) {
                return lhs.equals(BigDecimal.valueOf(rhs.byteValue()));
            } else if (rhs instanceof BigInteger) {
                return lhs.equals(new BigDecimal((BigInteger) rhs));
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
            } else if (rhs instanceof BigInteger) {
                return BigInteger.valueOf(lhsNumber.longValue()).compareTo((BigInteger) rhs);
            } else if (rhs instanceof BigDecimal) {
                return BigDecimal.valueOf(lhsNumber.longValue()).compareTo((BigDecimal) rhs);
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
            } else if (rhs instanceof BigInteger) {
                return new BigDecimal(lhsNumber.doubleValue()).compareTo(new BigDecimal((BigInteger) rhs));
            } else if (rhs instanceof BigDecimal) {
                return new BigDecimal(lhsNumber.doubleValue()).compareTo((BigDecimal) rhs);
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
            } else if (rhs instanceof BigInteger) {
                return BigInteger.valueOf(lhsNumber.intValue()).compareTo((BigInteger) rhs);
            } else if (rhs instanceof BigDecimal) {
                return BigDecimal.valueOf(lhsNumber.intValue()).compareTo((BigDecimal) rhs);
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
            } else if (rhs instanceof BigInteger) {
                return new BigDecimal(lhsNumber.floatValue()).compareTo(new BigDecimal((BigInteger) rhs));
            } else if (rhs instanceof BigDecimal) {
                return new BigDecimal(lhsNumber.floatValue()).compareTo((BigDecimal) rhs);
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
            } else if (rhs instanceof BigInteger) {
                return BigInteger.valueOf(lhsNumber.shortValue()).compareTo((BigInteger) rhs);
            } else if (rhs instanceof BigDecimal) {
                return BigDecimal.valueOf(lhsNumber.shortValue()).compareTo((BigDecimal) rhs);
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
            } else if (rhs instanceof BigInteger) {
                return BigInteger.valueOf(lhsNumber.shortValue()).compareTo((BigInteger) rhs);
            } else if (rhs instanceof BigDecimal) {
                return BigDecimal.valueOf(lhsNumber.shortValue()).compareTo((BigDecimal) rhs);
            }
        } else if (lhs instanceof BigInteger) {
            if (rhsClass == Long.class) {
                return lhs.compareTo(BigInteger.valueOf(rhsNumber.longValue()));
            } else if (rhsClass == Double.class) {
                return new BigDecimal((BigInteger) lhs).compareTo(new BigDecimal(rhsNumber.doubleValue()));
            } else if (rhsClass == Integer.class) {
                return lhs.compareTo(BigInteger.valueOf(rhsNumber.intValue()));
            } else if (rhsClass == Float.class) {
                return new BigDecimal((BigInteger) lhs).compareTo(new BigDecimal(rhsNumber.floatValue()));
            } else if (rhsClass == Short.class) {
                return new BigDecimal((BigInteger) lhs).compareTo(BigDecimal.valueOf(rhsNumber.shortValue()));
            } else if (rhsClass == Byte.class) {
                return new BigDecimal((BigInteger) lhs).compareTo(BigDecimal.valueOf(rhsNumber.byteValue()));
            } else if (rhs instanceof BigDecimal) {
                return new BigDecimal((BigInteger) lhs).compareTo((BigDecimal) rhs);
            }
        } else if (lhs instanceof BigDecimal) {
            if (rhsClass == Long.class) {
                return lhs.compareTo(BigDecimal.valueOf(rhsNumber.longValue()));
            } else if (rhsClass == Double.class) {
                return lhs.compareTo(new BigDecimal(rhsNumber.doubleValue()));
            } else if (rhsClass == Integer.class) {
                return lhs.compareTo(BigDecimal.valueOf(rhsNumber.intValue()));
            } else if (rhsClass == Float.class) {
                return lhs.compareTo(new BigDecimal(rhsNumber.floatValue()));
            } else if (rhsClass == Short.class) {
                return lhs.compareTo(BigDecimal.valueOf(rhsNumber.shortValue()));
            } else if (rhsClass == Byte.class) {
                return lhs.compareTo(BigDecimal.valueOf(rhsNumber.byteValue()));
            } else if (rhs instanceof BigInteger) {
                return lhs.compareTo(new BigDecimal((BigInteger) rhs));
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

}
