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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.sql.impl.QueryException;

import java.util.Locale;

/**
 * Utility methods for string functions.
 */
public final class StringExpressionUtils {
    private StringExpressionUtils() {
        // No-op.
    }

    public static String replace(String source, String search, String replacement) {
        if (source == null || search == null || replacement == null) {
            return null;
        }

        if (search.isEmpty()) {
            throw QueryException.error("Invalid operand: search cannot be empty.");
        }

        return source.replace(search, replacement);
    }

    public static String concat(String first, String second) {
        return (first != null ? first : "") + (second != null ? second : "");
    }

    public static Integer position(String seek, String source, int position) {
        if (seek == null) {
            return null;
        }

        if (source == null) {
            return null;
        }

        if (position == 0) {
            return source.indexOf(seek) + 1;
        } else {
            int position0 = position - 1;

            if (position0 < 0 || position0 > source.length()) {
                return 0;
            }

            return source.indexOf(seek, position0) + 1;
        }
    }

    public static String substring(String source, Integer startPos, Integer length) {
        // TODO: Validate the implementation against ANSI.
        if (source == null || startPos == null || length == null) {
            return null;
        }

        int sourceLength = source.length();

        if (startPos < 0) {
            startPos += sourceLength + 1;
        }

        int endPos = startPos + length;

        if (endPos < startPos) {
            throw QueryException.error("End position is less than start position.");
        }

        if (startPos > sourceLength || endPos < 1) {
            return "";
        }

        int startPos0 = Math.max(startPos, 1);
        int endPos0 = Math.min(endPos, sourceLength + 1);

        return source.substring(startPos0 - 1, endPos0 - 1);
    }

    public static Integer charLength(String value) {
        return value != null ? value.length() : null;
    }

    public static Integer ascii(String value) {
        return value == null ? null : value.isEmpty() ? 0 : value.codePointAt(0);
    }

    public static String upper(String value) {
        return value != null ? value.toUpperCase(Locale.ROOT) : null;
    }

    public static String lower(String value) {
        return value != null ? value.toLowerCase(Locale.ROOT) : null;
    }

    private static String initcap(String value) {
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

            if (Character.isWhitespace(c)) {
                res.append(c);

                capitalizeNext = true;
            } else if (capitalizeNext) {
                res.append(Character.toTitleCase(c));

                capitalizeNext = false;
            } else {
                res.append(c);
            }
        }

        return res.toString();
    }
}
