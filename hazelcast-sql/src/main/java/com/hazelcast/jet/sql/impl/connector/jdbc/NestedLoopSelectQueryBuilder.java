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

import com.hazelcast.sql.impl.schema.TableField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class NestedLoopSelectQueryBuilder extends SelectQueryBuilder {

    public NestedLoopSelectQueryBuilder(JdbcTable table,
                                        SqlDialect dialect,
                                        RexNode predicate,
                                        List<RexNode> projection,
                                        int[] rightEquiJoinIndices) {
        super(table, dialect, predicate, projection);

        StringBuilder stringBuilder = new StringBuilder(this.query);
        appendIndices(stringBuilder, rightEquiJoinIndices);
        query = stringBuilder.toString();
    }

    private void appendIndices(StringBuilder stringBuilder, int[] rightEquiJoinIndices) {
        stringBuilder.append(" WHERE ");

        String whereClause = Arrays.stream(rightEquiJoinIndices)
                .mapToObj(index -> {
                    TableField tableField = jdbcTable.getField(index);
                    return tableField.getName() + " = ? ";
                })
                .collect(Collectors.joining("AND "));
        stringBuilder.append(whereClause);
    }
}
