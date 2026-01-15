/*
 * Copyright 2026 Hazelcast Inc.
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

package com.hazelcast.sql.impl;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

/**
 * Basic functions for quoting identifiers in SQL queries using {@link CalciteSqlDialect#DEFAULT}.
 * <p>
 * Prevents using Calcite's classes outside {@code hazelcast-sql}, meaning other modules won't need to relocate Calcite as
 * {@code hazelcast-sql}  does.
 */
public final class QuoteIdentifierUtil {

    private static final SqlDialect DIALECT = CalciteSqlDialect.DEFAULT;

    private QuoteIdentifierUtil() {
    }

    /**
     * Encloses an identifier in quotation marks appropriate for the {@link CalciteSqlDialect SQL
     * dialect}, writing the result to a {@link StringBuilder}.
     * Wraps CalciteSqlDialect#quoteIdentifier.
     */
    public static StringBuilder quoteIdentifier(StringBuilder sb, String value) {
        return DIALECT.quoteIdentifier(sb, value);
    }

    /**
     * Appends a string literal to a buffer.
     * Wraps CalciteSqlDialect#quoteStringLiteral.
     */
    public static void quoteStringLiteral(StringBuilder buf, @Nullable String charsetName,
                                   String val) {
       DIALECT.quoteStringLiteral(buf, charsetName, val);
    }

    /**
     * Quote the given compound identifier using Calcite dialect (= Hazelcast dialect).
     * You can use this when giving information to the user, e.g. in exception message.
     * When building a query you should use {@link SqlDialect#quoteIdentifier(StringBuilder, List)} directly.
     */
    public static String quoteCompoundIdentifier(String... compoundIdentifier) {
        List<String> parts = Arrays.stream(compoundIdentifier).filter(Objects::nonNull).collect(toList());
        return CalciteSqlDialect.DEFAULT
                .quoteIdentifier(new StringBuilder(), parts)
                .toString();
    }
}
