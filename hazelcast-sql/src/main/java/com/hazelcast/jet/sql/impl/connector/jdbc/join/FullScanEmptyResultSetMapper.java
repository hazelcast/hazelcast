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

package com.hazelcast.jet.sql.impl.connector.jdbc.join;

import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.JetSqlRow;

import java.util.List;
import java.util.function.Supplier;

/**
 * This class is used when the ResultSet is empty. Similar to a Row Mapper this class also
 * knows SQL Join information.
 */
public class FullScanEmptyResultSetMapper implements Supplier<JetSqlRow> {

    private final JetJoinInfo joinInfo;

    private final List<Expression<?>> projections;
    private final JetSqlRow leftRow;

    public FullScanEmptyResultSetMapper(List<Expression<?>> projections, JetJoinInfo joinInfo, JetSqlRow leftRow) {
        this.joinInfo = joinInfo;
        this.projections = projections;
        this.leftRow = leftRow;
    }

    @Override
    public JetSqlRow get() {
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
