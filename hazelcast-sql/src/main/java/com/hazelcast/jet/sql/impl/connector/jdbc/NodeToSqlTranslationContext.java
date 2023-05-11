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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.rel2sql.SqlImplementor.Context;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.function.IntFunction;

/**
 * Custom variant of
 * {@code org.apache.calcite.rel.rel2sql.SqlImplementor.SimpleContext}
 * that also provides {@link #implementor()} which is required to
 * translate some more complex expressions, such as {@link SqlStdOperatorTable#SEARCH}.
 */
class NodeToSqlTranslationContext extends Context {

    private final IntFunction<SqlNode> field;
    private final RelToSqlConverter converter;

    NodeToSqlTranslationContext(SqlDialect dialect, IntFunction<SqlNode> field) {
        // TODO: do not generate redundant casts when DB has implicit cast.
        //  This may be dialect specific and affect index usage.
        super(dialect, 0, false);
        this.field = field;
        converter = new RelToSqlConverter(dialect);
    }

    @Override
    public SqlImplementor implementor() {
        return converter;
    }

    @Override
    public SqlNode field(int ordinal) {
        return field.apply(ordinal);
    }
}
