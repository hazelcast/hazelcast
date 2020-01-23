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

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.TriCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * LIKE string function.
 */
public class LikeFunction extends TriCallExpression<Boolean> {
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

    /** Source type. */
    private transient DataType sourceType;

    /** Patternt type. */
    private transient DataType patternType;

    /** Escape type. */
    private transient DataType escapeType;

    /** Last observed pattern. */
    private transient String lastPattern;

    /** Last observed escape. */
    private transient String lastEscape;

    /** Last Java pattern. */
    private transient Pattern lastJavaPattern;

    public LikeFunction() {
        // No-op.
    }

    public LikeFunction(Expression operand1, Expression operand2, Expression operand3) {
        super(operand1, operand2, operand3);
    }

    @Override
    public Boolean eval(QueryContext ctx, Row row) {
        String source;
        String pattern;
        String escape;

        // Get source operand.
        Object sourceValue = operand1.eval(ctx, row);

        if (sourceValue == null) {
            return null;
        }

        if (sourceType == null) {
            sourceType = operand1.getType();
        }

        source = sourceType.getConverter().asVarchar(sourceValue);

        // Get search operand.
        Object patternValue = operand2.eval(ctx, row);

        if (patternValue == null) {
            return null;
        }

        if (patternType == null) {
            patternType = operand2.getType();
        }

        pattern = patternType.getConverter().asVarchar(patternValue);

        // Get replacement operand.
        Object escapeValue = operand3 != null ? operand3.eval(ctx, row) : null;

        if (escapeValue != null) {
            if (escapeType == null) {
                escapeType = operand3.getType();
            }

            escape = escapeType.getConverter().asVarchar(escapeValue);
        } else {
            escape = null;
        }

        // Process.
        return like(source, pattern, escape);
    }

    // TODO: Validate the implementation against ANSI.
    // TODO: Since pattern and escape are most often constants, optimzie for this case so that the compilation happens only once.
    private boolean like(String source, String pattern, String escape) {
        Pattern javaPattern = convertToJavaPattern(pattern, escape);

        Matcher matcher = javaPattern.matcher(source);

        return matcher.matches();
    }

    private Pattern convertToJavaPattern(String pattern, String escape) {
        if (Objects.equals(pattern, lastPattern) && Objects.equals(escape, lastEscape) && lastJavaPattern != null) {
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
                throw new HazelcastSqlException(-1, "Escape parameter should be a single character: " + escape);
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
                    throw new HazelcastSqlException(-1, "Escape symbol cannot be located at the end of the pattern.");
                }

                char nextPatternChar = pattern.charAt(i + 1);

                if ((nextPatternChar == ONE_SQL) || (nextPatternChar == MANY_SQL) || (nextPatternChar == escapeChar)) {
                    javaPattern.append(nextPatternChar);

                    i++;
                } else {
                    throw new HazelcastSqlException(-1, "Escape should be applied only to '_', '%' or escape symbols.");
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

    @Override
    public int operator() {
        return CallOperator.LIKE;
    }

    @Override
    public DataType getType() {
        return DataType.BIT;
    }
}
