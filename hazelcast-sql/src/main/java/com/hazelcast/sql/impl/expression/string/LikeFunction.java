/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.ConcurrentInitialSetCache;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.TriExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.sql.impl.expression.string.StringFunctionUtils.asVarchar;

/**
 * LIKE string function.
 */
public class LikeFunction extends TriExpression<Boolean> {
    private static final int PATTERN_CACHE_SIZE = 100;

    private static final long serialVersionUID = 2L;

    /** Single-symbol wildcard in SQL. */
    private static final char ONE_SQL = '_';

    /** Multi-symbol wildcard in SQL. */
    private static final char MANY_SQL = '%';

    /** Single-symbol wildcard in Java. */
    private static final String ONE_JAVA = ".";

    /** Multi-symbol wildcard in Java. */
    private static final String MANY_JAVA = ".*";

    /** Special characters which require escaping in Java. */
    private static final String ESCAPE_CHARACTERS_JAVA = "[]()|^+*?{}$\\.";

    private boolean negated;

    private transient ConcurrentInitialSetCache<Tuple2<String, String>, Pattern> patternCache;

    public LikeFunction() {
        // No-op.
    }

    private LikeFunction(Expression<?> source, Expression<?> pattern, Expression<?> escape, boolean negated) {
        super(source, pattern, escape);

        this.negated = negated;
        this.patternCache = new ConcurrentInitialSetCache<>(PATTERN_CACHE_SIZE);
    }

    public static LikeFunction create(
            Expression<?> source,
            Expression<?> pattern,
            Expression<?> escape,
            boolean negated
    ) {
        return new LikeFunction(source, pattern, escape, negated);
    }

    @SuppressFBWarnings(value = "NP_BOOLEAN_RETURN_NULL", justification = "SQL has three-valued boolean logic")
    @Override
    public Boolean eval(Row row, ExpressionEvalContext context) {
        String source = asVarchar(operand1, row, context);

        if (source == null) {
            return null;
        }

        String pattern = asVarchar(operand2, row, context);

        if (pattern == null) {
            return null;
        }

        String escape;

        if (operand3 != null) {
            escape = asVarchar(operand3, row, context);

            if (escape == null) {
                return null;
            }
        } else {
            escape = null;
        }

        boolean res = like(source, pattern, escape);

        if (negated) {
            res = !res;
        }

        return res;
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.BOOLEAN;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_LIKE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeBoolean(negated);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        negated = in.readBoolean();
        patternCache = new ConcurrentInitialSetCache<>(PATTERN_CACHE_SIZE);
    }

    private void readObject(ObjectInputStream stream) throws ClassNotFoundException, IOException {
        stream.defaultReadObject();
        // The transient fields are not initialized during Java deserialization, so we need to do it manually.
        patternCache = new ConcurrentInitialSetCache<>(PATTERN_CACHE_SIZE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        LikeFunction that = (LikeFunction) o;

        return negated == that.negated;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), negated);
    }

    public boolean like(String source, String pattern, String escape) {
        Pattern javaPattern = convertToJavaPattern(pattern, escape);

        Matcher matcher = javaPattern.matcher(source);

        return matcher.matches();
    }

    private Pattern convertToJavaPattern(String pattern, String escape) {
        Tuple2<String, String> cacheKey = Tuple2.tuple2(pattern, escape);
        return patternCache.computeIfAbsent(cacheKey, key -> {
            String javaPatternStr = constructJavaPatternString(pattern, escape);
            return Pattern.compile(javaPatternStr, Pattern.DOTALL);
        });
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private static String constructJavaPatternString(String pattern, String escape) {
        // Get the escape character.
        Character escapeChar;

        if (escape != null) {
            if (escape.length() != 1) {
                throw QueryException.error("ESCAPE parameter must be a single character");
            }

            escapeChar = escape.charAt(0);
        } else {
            escapeChar = null;
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

            if (escapeChar != null && patternChar == escapeChar) {
                if (i == (pattern.length() - 1)) {
                    throw escapeWildcardsOnly();
                }

                char nextPatternChar = pattern.charAt(i + 1);

                if ((nextPatternChar == ONE_SQL) || (nextPatternChar == MANY_SQL) || (nextPatternChar == escapeChar)) {
                    javaPattern.append(nextPatternChar);

                    i++;
                } else {
                    throw escapeWildcardsOnly();
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

    private static QueryException escapeWildcardsOnly() {
        return QueryException.error("Only '_', '%' and the escape character can be escaped");
    }
}
