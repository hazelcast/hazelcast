package com.hazelcast.jet.sql.impl.connector.jdbc.fullscanresultsetstream;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class FullScanRowMapper implements FunctionEx<ResultSet, JetSqlRow> {

    private final ExpressionEvalContext expressionEvalContext;

    private final JetJoinInfo joinInfo;

    private final List<Expression<?>> projections;
    private final JetSqlRow leftRow;

    private Object[] values;

    public FullScanRowMapper(ExpressionEvalContext expressionEvalContext,
                             List<Expression<?>> projections,
                             JetJoinInfo joinInfo,
                             JetSqlRow leftRow) {
        this.expressionEvalContext = expressionEvalContext;
        this.projections = projections;
        this.joinInfo = joinInfo;
        this.leftRow = leftRow;
    }

    @Override
    public JetSqlRow applyEx(ResultSet resultSet) throws SQLException {
        if (values == null) {
            values = createValueArray(resultSet);
        }
        fillValueArray(resultSet, values);

        JetSqlRow jetSqlRowFromDB = new JetSqlRow(
                expressionEvalContext.getSerializationService(),
                values);

        // Join the leftRow with the row from DB
        JetSqlRow joinedRow = ExpressionUtil.join(leftRow,
                jetSqlRowFromDB,
                joinInfo.nonEquiCondition(),
                expressionEvalContext);

        if (joinedRow != null) {
            // The DB row evaluated as true
            return joinedRow;
        } else {
            // The DB row evaluated as false
            if (!joinInfo.isInner()) {
                // This is not an inner join, so return a null padded JetSqlRow
                return createExtendedRow(leftRow);
            }
        }
        return null;
    }

    protected static Object[] createValueArray(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        return new Object[columnCount];
    }

    protected static void fillValueArray(ResultSet resultSet, Object[] values) throws SQLException {
        for (int index = 0; index < values.length; index++) {
            // TODO we need to use mechanism similar (or maybe the same) to valueGetters in SelectProcessorSupplier
            values[index] = resultSet.getObject(index + 1);
        }
    }
    protected JetSqlRow createExtendedRow(JetSqlRow leftRow) {
        return leftRow.extendedRow(projections.size());
    }
}
