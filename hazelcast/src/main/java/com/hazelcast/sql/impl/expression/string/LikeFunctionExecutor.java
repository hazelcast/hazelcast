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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper class to execute LIKE function. Caches the last observed pattern to avoid constant re-compilation.
 */
public class LikeFunctionExecutor {
    /** Single-symbol wildcard in SQL. */
    private static final char ONE_SQL = '_';

    /** Multi-symbol wildcard in SQL. */
    private static final char MANY_SQL = '%';

    /** Single-symbol wildcard in Java. */
    private static final String ONE_JAVA = ".";

    /** Multi-symbol wildcard in Java. */
    private static final String MANY_JAVA = "(?s:.*)";

    /** Special characters which require escaping in Java. */
    private static final String ESCAPE_CHARACTERS_JAVA = "[]()|^-+*?{}$\\.";


    /** Last observed pattern. */
    private String lastPattern;

    /** Last observed escape. */
    private String lastEscape;

    /** Last Java pattern. */
    private Pattern lastJavaPattern;

    @SuppressFBWarnings(value = "NP_BOOLEAN_RETURN_NULL", justification = "SQL ternary logic allows NULL")
    public Boolean like(String source, String pattern, String escape) {
        // TODO: Validate the implementation against ANSI.
        if (source == null || pattern == null) {
            return null;
        }

        Pattern javaPattern = convertToJavaPattern(pattern, escape);

        Matcher matcher = javaPattern.matcher(source);

        return matcher.matches();
    }

    private Pattern convertToJavaPattern(String pattern, String escape) {
        if (lastJavaPattern != null && Objects.equals(pattern, lastPattern) && Objects.equals(escape, lastEscape)) {
            return lastJavaPattern;
        }

        String javaPatternStr = constructJavaPatternString(pattern, escape);
        Pattern javaPattern = Pattern.compile(javaPatternStr);

        lastPattern = pattern;
        lastEscape = escape;
        lastJavaPattern = javaPattern;

        return javaPattern;
    }

    private static String constructJavaPatternString(String pattern, String escape) {
        // Get the escape character.
        char escapeChar;

        if (escape != null) {
            if (escape.length() != 1) {
                throw QueryException.error("Escape parameter should be a single character: " + escape);
            }

            escapeChar = escape.charAt(0);
        } else {
            escapeChar = 0;
        }

        // Main logic.
        StringBuilder javaPattern = new StringBuilder();

        int i;

        for (i = 0; i < pattern.length(); i++) {
            char patternChar = pattern.charAt(i);

            // Escape special character as needed.
            if (ESCAPE_CHARACTERS_JAVA.indexOf(patternChar) >= 0) {
                javaPattern.append('\\');
            }

            if (patternChar == escapeChar) {
                if (i == (pattern.length() - 1)) {
                    throw QueryException.error("Escape symbol cannot be located at the end of the pattern.");
                }

                char nextPatternChar = pattern.charAt(i + 1);

                if ((nextPatternChar == ONE_SQL) || (nextPatternChar == MANY_SQL) || (nextPatternChar == escapeChar)) {
                    javaPattern.append(nextPatternChar);

                    i++;
                } else {
                    throw QueryException.error("Escape should be applied only to '_', '%' or escape symbols.");
                }
            } else if (patternChar == ONE_SQL) {
                javaPattern.append(ONE_JAVA);
            } else if (patternChar == MANY_SQL) {
                javaPattern.append(MANY_JAVA);
            } else {
                javaPattern.append(patternChar);
            }
        }

        return javaPattern.toString();
    }
}
