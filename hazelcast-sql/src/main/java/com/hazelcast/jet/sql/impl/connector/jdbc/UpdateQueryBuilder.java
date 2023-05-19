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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class UpdateQueryBuilder extends AbstractQueryBuilder {

    private final List<Integer> dynamicParams = new ArrayList<>();
    private final List<Integer> inputRefs = new ArrayList<>();

    UpdateQueryBuilder(
            JdbcTable table,
            SqlDialect dialect,
            List<String> fieldNames,
            List<RexNode> expressions,
            RexNode predicate,
            boolean hasInput
    ) {
        super(table, dialect);

        assert fieldNames.size() == expressions.size();

        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        dialect.quoteIdentifier(sb, Arrays.asList(table.getExternalName()));

        sb.append(" SET ");
        ParamCollectingVisitor dynamicParamVisitor = new ParamCollectingVisitor(dynamicParams);
        for (int i = 0; i < fieldNames.size(); i++) {
            RexNode rexNode = expressions.get(i);

            SqlNode sqlNode = context.toSql(null, rexNode);
            sqlNode.accept(dynamicParamVisitor);

            String externalFieldName = table.getField(fieldNames.get(i)).externalName();
            dialect.quoteIdentifier(sb, externalFieldName);
            sb.append('=');
            sb.append(sqlNode.toSqlString(dialect).toString());
            if (i < fieldNames.size() - 1) {
                sb.append(", ");
            }
        }

        if (predicate != null) {
            appendPredicate(sb, predicate, dynamicParams);
        } else if (hasInput) {
            appendPrimaryKeyPredicate(sb, inputRefs);
        }

        query = sb.toString();
    }

    int[] dynamicParams() {
        return Ints.toArray(dynamicParams);
    }

    int[] inputRefs() {
        return Ints.toArray(inputRefs);
    }
}
