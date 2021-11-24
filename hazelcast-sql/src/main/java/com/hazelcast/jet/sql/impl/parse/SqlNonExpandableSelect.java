/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.parse;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

/*
 * Special SqlSelect AST node class.
 * It is used as CREATE VIEW SELECT query alias.
 * During the validation phase it would be expanded only on top level.
 * Example :
 * ```
 * CREATE VIEW v AS SELECT 1;
 * CREATE VIEW vv AS SELECT * v
 *
 * -- this query
 * SELECT * FROM vv
 * -- it would be expanded to
 * SELECT * FROM (SELECT * FROM v)
 * -- but not to
 * SELECT * FROM (SELECT * FROM (SELECT 1)))
 * ```
 */
public class SqlNonExpandableSelect extends SqlSelect {
    @SuppressWarnings("checkstyle:ParameterNumber")
    public SqlNonExpandableSelect(
            SqlParserPos pos,
            @Nullable SqlNodeList keywordList,
            SqlNodeList selectList,
            @Nullable SqlNode from,
            @Nullable SqlNode where,
            @Nullable SqlNodeList groupBy,
            @Nullable SqlNode having,
            @Nullable SqlNodeList windowDecls,
            @Nullable SqlNodeList orderBy,
            @Nullable SqlNode offset,
            @Nullable SqlNode fetch,
            @Nullable SqlNodeList hints) {
        super(pos, keywordList, selectList, from, where, groupBy, having, windowDecls, orderBy, offset, fetch, hints);
    }

    public SqlNonExpandableSelect(SqlSelect select) {
        super(
                select.getParserPosition(),
                (SqlNodeList) select.getOperandList().get(0),
                select.getSelectList(),
                select.getFrom(),
                select.getWhere(),
                select.getGroup(),
                select.getHaving(),
                select.getWindowList(),
                select.getOrderList(),
                select.getOffset(),
                select.getFetch(),
                select.getHints()
        );
    }
}
