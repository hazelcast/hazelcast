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

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class SqlCreateType extends SqlCreate {
    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE TYPE", SqlKind.CREATE_TYPE);

    private final SqlIdentifier name;
    private final SqlNodeList columns;
    private final SqlNodeList options;

    public SqlCreateType(
            final SqlIdentifier name,
            final SqlNodeList columns,
            final SqlNodeList options,
            final boolean replace,
            final boolean ifNotExists,
            final SqlParserPos pos
    ) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.name = requireNonNull(name, "Name should not be null");
        this.columns = requireNonNull(columns, "Columns should not be null");
        this.options = requireNonNull(options, "Options should not be null");
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columns, options);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void validate(final SqlValidator validator, final SqlValidatorScope scope) {
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (getReplace()) {
            writer.keyword("OR REPLACE");
        }

        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }

        name.unparse(writer, leftPrec, rightPrec);

        if (!columns.isEmpty()) {
            writer.keyword("(");
            for (final SqlNode column : columns) {
                writer.sep(",", false);
                column.unparse(writer, leftPrec, rightPrec);
            }
            writer.keyword(")");
        }

        if (options().isEmpty()) {
            return;
        }

        writer.keyword("OPTIONS");
        writer.keyword("(");
        for (final SqlNode option : options) {
            writer.sep(",", false);
            option.unparse(writer, leftPrec, rightPrec);
        }
        writer.keyword(")");
    }

    public String getName() {
        return name.getSimple();
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    public Stream<SqlTypeColumn> columns() {
        return columns.getList().stream().map(node -> (SqlTypeColumn) node);
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
}
