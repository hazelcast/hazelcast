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

import com.hazelcast.sql.impl.schema.type.Type;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.parse.ParserResource.RESOURCE;
import static com.hazelcast.jet.sql.impl.parse.UnparseUtil.identifier;
import static com.hazelcast.jet.sql.impl.parse.UnparseUtil.nodeList;
import static com.hazelcast.jet.sql.impl.parse.UnparseUtil.printIndent;
import static com.hazelcast.jet.sql.impl.parse.UnparseUtil.reconstructOptions;
import static com.hazelcast.jet.sql.impl.parse.UnparseUtil.unparseOptions;
import static com.hazelcast.jet.sql.impl.validate.ValidationUtil.isCatalogObjectNameValid;
import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PUBLIC;
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
        if (getReplace() && ifNotExists) {
            throw validator.newValidationError(this, RESOURCE.orReplaceWithIfNotExistsNotSupported());
        }

        if (!isCatalogObjectNameValid(name)) {
            throw validator.newValidationError(name, RESOURCE.typeIncorrectSchema());
        }

        final Set<String> columnNames = new HashSet<>();
        for (SqlNode column : columns.getList()) {
            String name = ((SqlTypeColumn) column).name();
            if (!columnNames.add(name)) {
                throw validator.newValidationError(column, RESOURCE.duplicateColumn(name));
            }
        }

        final Set<String> optionNames = new HashSet<>();
        for (SqlNode option : options.getList()) {
            String name = ((SqlOption) option).keyString();
            if (!optionNames.add(name)) {
                throw validator.newValidationError(option, RESOURCE.duplicateOption(name));
            }
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (getReplace()) {
            writer.keyword("OR REPLACE");
        }
        writer.keyword("TYPE");
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }

        name.unparse(writer, leftPrec, rightPrec);

        if (!columns.isEmpty()) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (final SqlNode column : columns) {
                printIndent(writer);
                column.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(frame);
        }

        unparseOptions(writer, options);
    }

    public static String unparse(Type type) {
        SqlPrettyWriter writer = new SqlPrettyWriter(SqlPrettyWriter.config());

        SqlCreateType t = new SqlCreateType(
                identifier(CATALOG, SCHEMA_NAME_PUBLIC, type.name()),
                nodeList(type.getFields(), f -> new SqlTypeColumn(
                        identifier(f.getName()), new SqlDataType(f.getType(), SqlParserPos.ZERO), SqlParserPos.ZERO)),
                reconstructOptions(type.options()),
                true, false, SqlParserPos.ZERO);

        t.unparse(writer, 0, 0);
        return writer.toString();
    }

    public String typeName() {
        return name.names.get(name.names.size() - 1);
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
