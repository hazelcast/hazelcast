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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.impl.function.SecuredFunction;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

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
            @Nonnull JetJoinInfo joinInfo,
            List<Expression<?>> projections) {
        super(dataConnectionName, query, joinInfo, projections);
        this.query = query;
        this.joinInfo = joinInfo;
        this.projections = projections;
    }

    protected Traverser<JetSqlRow> joinRows(Iterable<JetSqlRow> leftRows) {
        List<JetSqlRow> resultRows = new ArrayList<>();

        for (JetSqlRow leftRow : leftRows) {
            joinRow(leftRow, resultRows);
        }
        return traverseIterable(resultRows);
    }

    private void joinRow(JetSqlRow leftRow, List<JetSqlRow> resultRows) {

        // Full scan : Select * from the table and iterate over the ResulSet
        try (Connection connection = dataConnection.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            Object[] values = createValueArray(resultSet);

            boolean emptyResultSet = true;

            while (resultSet.next()) {
                emptyResultSet = false;
                fillValueArray(resultSet, values);

                JetSqlRow jetSqlRowFromDB = new JetSqlRow(expressionEvalContext.getSerializationService(), values);

                // Join the leftRow with the row from DB
                JetSqlRow joinedRow = ExpressionUtil.join(leftRow, jetSqlRowFromDB, joinInfo.nonEquiCondition(),
                        expressionEvalContext);
                if (joinedRow != null) {
                    // The DB row evaluated as true
                    resultRows.add(joinedRow);
                } else {
                    // The DB row evaluated as false
                    if (!joinInfo.isInner()) {
                        // This is not an inner join, so return a null padded JetSqlRow
                        createExtendedRowIfNecessary(leftRow, resultRows);
                    }
                }
            }
            if (emptyResultSet) {
                createExtendedRowIfNecessary(leftRow, resultRows);
            }
        } catch (SQLException e) {
            throw rethrow(e);
        }
    }

}
