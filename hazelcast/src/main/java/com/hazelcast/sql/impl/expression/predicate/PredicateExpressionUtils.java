/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.predicate;

public final class PredicateExpressionUtils {
    private PredicateExpressionUtils() {
        // No-op.
    }

    public static Boolean and(Boolean first, Boolean second) {
        if (first == Boolean.FALSE || second == Boolean.FALSE) {
            return Boolean.FALSE;
        }

        if (first == null || second == null) {
            return null;
        }

        return Boolean.TRUE;
    }

    public static Boolean or(Boolean first, Boolean second) {
        if (first == Boolean.TRUE || second == Boolean.TRUE) {
            return Boolean.TRUE;
        }

        if (first == null || second == null) {
            return null;
        }

        return Boolean.FALSE;
    }

    public static Boolean not(Boolean val) {
        return val != null ? !val : null;
    }

    public static boolean isNull(Object val) {
        return val == null;
    }

    public static boolean isNotNull(Object val) {
        return val != null;
    }

    public static boolean isTrue(Boolean val) {
        return val != null && val;
    }

    public static boolean isNotTrue(Boolean val) {
        return !isTrue(val);
    }

    public static boolean isFalse(Boolean val) {
        return val != null && !val;
    }

    public static boolean isNotFalse(Boolean val) {
        return !isFalse(val);
    }
}
