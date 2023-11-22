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

import com.google.common.primitives.Ints;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlString;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class SelectQueryBuilder extends AbstractQueryBuilder {

    private final List<Integer> dynamicParams = new ArrayList<>();
    private final List<FunctionEx<Object, ?>> converters = new ArrayList<>();

    SelectQueryBuilder(JdbcTable table, SqlDialect dialect, RexNode predicate, List<RexNode> projection) {
        super(table, dialect);

        StringBuilder sb = new StringBuilder();
        selectClause(sb, projection);
        fromClause(sb, predicate);

        query = sb.toString();
    }

    protected void selectClause(StringBuilder sb, List<RexNode> projection) {
        sb.append("SELECT ");
        if (!projection.isEmpty()) {
            appendProjection(sb, projection);
        } else {
            sb.append("*");
        }
    }

    protected void appendProjection(StringBuilder sb, List<RexNode> projection) {
        Iterator<RexNode> it = projection.iterator();
        while (it.hasNext()) {
            RexNode node = it.next();
            if (node instanceof RexInputRef) {
                RexInputRef rexInputRef = (RexInputRef) node;
                SqlIdentifier field = (SqlIdentifier) context.field(rexInputRef.getIndex());
                QueryDataType fieldType = jdbcTable.getFieldByExternalName(field.getSimple()).getType();
                converters.add(fieldType::convert);
            } else {
                converters.add(FunctionEx.identity());
            }
            SqlNode sqlNode = context.toSql(null, node);
            SqlString sqlString = sqlNode.toSqlString(dialect);
            sb.append(sqlString.toString());
            if (it.hasNext()) {
                sb.append(',');
            }
        }
    }

    protected void fromClause(StringBuilder sb, RexNode predicate) {
        sb.append(" FROM ");
        dialect.quoteIdentifier(sb, jdbcTable.getExternalNameList());
        if (predicate != null) {
            appendPredicate(sb, predicate, dynamicParams);
        }
    }

    int[] parameterPositions() {
        return Ints.toArray(dynamicParams);
    }

    public List<FunctionEx<Object, ?>> converters() {
        return converters;
    }
}
