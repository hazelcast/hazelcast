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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;

import java.util.Locale;

/**
 * Utility methods for string functions.
 */
public final class StringFunctionUtils {
    private StringFunctionUtils() {
        // No-op.
    }

    public static String concat(String first, String second) {
        return first != null && second != null ? first + second : null;
    }

    public static Integer charLength(String value) {
        return value != null ? value.length() : null;
    }

    public static Integer ascii(String value) {
        return value != null ? value.isEmpty() ? 0 : value.codePointAt(0) : null;
    }

    public static String upper(String value) {
        return value != null ? value.toUpperCase(Locale.ROOT) : null;
    }

    public static String lower(String value) {
        return value != null ? value.toLowerCase(Locale.ROOT) : null;
    }

    public static String replace(String original, String from, String to) {
        if (original == null) {
            return null;
        }
        // Java 'replace' matches empty string with every possible cursor location
        // in the original string.
        // On the other hand, SQL replace function doesn't match empty string to
        // anything in the original string.
        if (from.isEmpty()) {
            return original;
        }
        return original.replace(from, to);
    }

    public static String initcap(String value) {
        if (value == null) {
            return null;
        }

        if (value.isEmpty()) {
            return value;
        }

        int strLen = value.length();

        StringBuilder res = new StringBuilder(strLen);

        boolean capitalizeNext = true;

        for (int i = 0; i < strLen; i++) {
            char c = value.charAt(i);

            if (!Character.isLetterOrDigit(c)) {
                res.append(c);

                capitalizeNext = true;
            } else if (capitalizeNext) {
                res.append(Character.toTitleCase(c));

                capitalizeNext = false;
            } else {
                res.append(Character.toLowerCase(c));
            }
        }

        return res.toString();
    }

    public static String asVarchar(Expression<?> expression, Row row, ExpressionEvalContext context) {
        Object res = expression.eval(row, context);

        if (res == null) {
            return null;
        }

        return expression.getType().getConverter().asVarchar(res);
    }

    public static Integer search(String text, String search, int start) {
        return text.indexOf(search, start);
    }
}
