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
