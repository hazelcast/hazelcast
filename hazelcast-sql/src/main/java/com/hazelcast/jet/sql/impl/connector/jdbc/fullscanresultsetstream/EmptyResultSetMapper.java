package com.hazelcast.jet.sql.impl.connector.jdbc.fullscanresultsetstream;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.JetSqlRow;

import java.util.List;

public class EmptyResultSetMapper implements SupplierEx<JetSqlRow> {

    private final JetJoinInfo joinInfo;

    private final List<Expression<?>> projections;
    private final JetSqlRow leftRow;

    public EmptyResultSetMapper(List<Expression<?>> projections, JetJoinInfo joinInfo, JetSqlRow leftRow) {
        this.joinInfo = joinInfo;
        this.projections = projections;
        this.leftRow = leftRow;
    }

    @Override
    public JetSqlRow getEx() {
        return createExtendedRowIfNecessary();
    }

    protected JetSqlRow createExtendedRowIfNecessary() {
        JetSqlRow extendedRow = null;
        if (!joinInfo.isInner()) {
            // This is not an inner join, so return a null padded JetSqlRow
            extendedRow = leftRow.extendedRow(projections.size());
        }
        return extendedRow;
    }
}
