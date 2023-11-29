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
import com.hazelcast.jet.sql.impl.connector.jdbc.join.JoinPredicatePreparedStatementSetter;
import com.hazelcast.jet.sql.impl.connector.jdbc.join.JoinPredicateScanResultSetIterator;
import com.hazelcast.jet.sql.impl.connector.jdbc.join.JoinPredicateScanRowMapper;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.impl.function.SecuredFunction;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This class retrieves the right-side data for a Join operation.
 * The SQL provided to this processor includes a WHERE clause.
 * <p>
 * This processor operates with batches of JetSqlRow instances coming from the processor on the left side.
 * For each JetSqlRow, the WHERE clause is populated, resulting in the generation of a new SQL statement.
 * For optimization purposes, all generated SQL statements are merged into a single statement using the UNION ALL clause.
 * <p>
 * For a visual explanation, refer to {@link IndexScanSelectQueryBuilder}.
 */
public class JdbcJoinPredicateScanProcessorSupplier
        extends AbstractJoinProcessorSupplier
        implements DataSerializable, SecuredFunction {

    // Classes conforming to DataSerializable should provide a no-arguments constructor.
    @SuppressWarnings("unused")
    public JdbcJoinPredicateScanProcessorSupplier() {
    }

    public JdbcJoinPredicateScanProcessorSupplier(
            @Nonnull String dataConnectionName,
            @Nonnull String query,
            List<FunctionEx<Object, ?>> converters, @Nonnull JetJoinInfo joinInfo,
            List<Expression<?>> projections) {
        super(dataConnectionName, query, converters, joinInfo, projections);
    }

    protected AutoCloseableTraverser<JetSqlRow> joinRows(Iterable<JetSqlRow> leftRows) {
        List<JetSqlRow> leftRowsList = convertIterableToArrayList(leftRows);
        String unionAllSql = generateSql(leftRowsList);
        return AutoCloseableTraversers.traverseAutoCloseableIterator(joinUnionAll(leftRowsList, unionAllSql));
    }

    private <T> ArrayList<T> convertIterableToArrayList(Iterable<T> iterable) {
        Stream<T> stream = StreamSupport.stream(iterable.spliterator(), false);
        return stream.collect(Collectors.toCollection(ArrayList::new));
    }

    private String generateSql(List<JetSqlRow> leftRowsList) {
        String delimiter = " UNION ALL ";
        String sql = IntStream.range(0, leftRowsList.size())
                .mapToObj(i -> query.replaceFirst(IndexScanSelectQueryBuilder.ROW_NUMBER,
                        String.valueOf(i)))
                .collect(Collectors.joining(delimiter));
        sql = sql + " ORDER BY " + IndexScanSelectQueryBuilder.ROW_NUMBER_ALIAS;
        return sql;
    }

    // Return an iterator that can traverse leftRowsList and ResultSet of unionAllSql
    private JoinPredicateScanResultSetIterator<JetSqlRow> joinUnionAll(List<JetSqlRow> leftRowsList, String unionAllSql) {
        Connection connection = dataConnection.getConnection();
        TypeResolver typeResolver = JdbcSqlConnector.typeResolver(connection);
        return new JoinPredicateScanResultSetIterator<>(
                dataConnection.getConnection(),
                unionAllSql,
                new JoinPredicateScanRowMapper(
                        expressionEvalContext,
                        typeResolver,
                        converters,
                        projections,
                        joinInfo,
                        leftRowsList
                ),
                new JoinPredicatePreparedStatementSetter(joinInfo, leftRowsList)
        );
    }
}
