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

package com.hazelcast.jet.sql.impl.connector.jdbc.joinindexscanresultsetstream;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.row.JetSqlRow;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class PreparedStatementSetter implements ConsumerEx<PreparedStatement> {

    private final JetJoinInfo joinInfo;

    private final List<JetSqlRow> leftRowsList;

    public PreparedStatementSetter(JetJoinInfo joinInfo, List<JetSqlRow> leftRowsList) {
        this.joinInfo = joinInfo;
        this.leftRowsList = leftRowsList;
    }

    @Override
    public void acceptEx(PreparedStatement preparedStatement) throws SQLException {
        setObjectsToPreparedStatement(preparedStatement);
    }

    private void setObjectsToPreparedStatement(PreparedStatement preparedStatement)
            throws SQLException {
        int[] leftEquiJoinIndices = joinInfo.leftEquiJoinIndices();

        // PreparedStatement parameter index starts from 1
        int parameterIndex = 1;

        // leftRow contains all left table columns used in the select statement
        // leftEquiJoinIndices contains index of columns used in the JOIN clause
        for (JetSqlRow leftRow : leftRowsList) {
            for (int leftEquiJoinIndexValue : leftEquiJoinIndices) {
                Object value = leftRow.get(leftEquiJoinIndexValue);
                preparedStatement.setObject(parameterIndex++, value);
            }
        }
    }
}
