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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.impl.AutoCloseableTraversers;
import com.hazelcast.jet.impl.util.AutoCloseableTraverser;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.jdbc.join.FullScanEmptyResultSetMapper;
import com.hazelcast.jet.sql.impl.connector.jdbc.join.FullScanResultSetIterator;
import com.hazelcast.jet.sql.impl.connector.jdbc.join.FullScanRowMapper;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.impl.function.SecuredFunction;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.util.List;

/**
 * This class retrieves the right-side data for a Join operation.
 * The SQL provided to this processor does not include a WHERE clause,
 * resulting in the retrieval of all records from the right-side tables
 * and performing a full scan of the right side.
 */
public class JdbcJoinFullScanProcessorSupplier
        extends AbstractJoinProcessorSupplier
        implements DataSerializable, SecuredFunction {

    // Classes conforming to DataSerializable should provide a no-arguments constructor.
    @SuppressWarnings("unused")
    public JdbcJoinFullScanProcessorSupplier() {
    }

    public JdbcJoinFullScanProcessorSupplier(
            @Nonnull String dataConnectionName,
            @Nonnull String query,
            @Nonnull List<FunctionEx<Object, ?>> converters,
            @Nonnull JetJoinInfo joinInfo,
            List<Expression<?>> projections) {
        super(dataConnectionName, query, converters, joinInfo, projections);
    }

    protected AutoCloseableTraverser<JetSqlRow> joinRows(Iterable<JetSqlRow> leftRows) {
        return AutoCloseableTraversers.traverseAutoCloseableIterator(leftRows.iterator())
                  .flatMapAutoCloseable(jetSqlRow -> AutoCloseableTraversers.traverseAutoCloseableIterator(joinRow(jetSqlRow)));
    }

    private FullScanResultSetIterator<JetSqlRow> joinRow(JetSqlRow leftRow) {
        // Full scan : Select * from the table and iterate over the ResulSet
        Connection connection = dataConnection.getConnection();
        TypeResolver typeResolver = JdbcSqlConnector.typeResolver(connection);
        return new FullScanResultSetIterator<>(
                connection,
                query,
                new FullScanRowMapper(expressionEvalContext, typeResolver, converters, projections, joinInfo, leftRow),
                new FullScanEmptyResultSetMapper(projections, joinInfo, leftRow)
        );
    }
}
