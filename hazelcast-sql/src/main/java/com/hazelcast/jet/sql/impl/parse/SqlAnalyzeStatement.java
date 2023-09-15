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

package com.hazelcast.jet.sql.impl.parse;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class SqlAnalyzeStatement extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("ANALYZE", SqlKind.OTHER);

    private SqlNode query;
    private final SqlNodeList options;

    public SqlAnalyzeStatement(SqlParserPos pos, SqlNode query, SqlNodeList options) {
        super(pos);
        this.query = query;
        this.options = requireNonNull(options, "Options should not be null");
    }

    public SqlNode getQuery() {
        return query;
    }

    public void setQuery(final SqlNode query) {
        this.query = query;
    }

    public Map<String, String> options() {
        return options.getList().stream()
                .map(node -> (SqlOption) node)
                .collect(
                        LinkedHashMap::new,
                        (map, option) -> map.putIfAbsent(option.keyString(), option.valueString()),
                        Map::putAll
                );
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(query);
    }

    @Override
    public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
        writer.keyword("ANALYZE");
        if (!options.isEmpty()) {
            UnparseUtil.unparseOptions(writer, "WITH OPTIONS", options);
        }

        query.unparse(writer, leftPrec, rightPrec);
    }
}
