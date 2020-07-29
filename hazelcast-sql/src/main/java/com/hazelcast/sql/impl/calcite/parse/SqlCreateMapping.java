/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.parse;

import com.google.common.collect.ImmutableList;
import com.hazelcast.sql.impl.QueryException;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.sql.impl.calcite.parse.ParserResource.RESOURCE;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class SqlCreateMapping extends SqlCreate {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE MAPPING", SqlKind.CREATE_TABLE);

    private final SqlIdentifier name;
    private final SqlNodeList columns;
    private final SqlIdentifier type;
    private final SqlNodeList options;

    public SqlCreateMapping(
            SqlIdentifier name,
            SqlNodeList columns,
            SqlIdentifier type,
            SqlNodeList options,
            boolean replace,
            boolean ifNotExists,
            SqlParserPos pos
    ) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.name = requireNonNull(name, "Name should not be null");
        this.columns = requireNonNull(columns, "Columns should not be null");
        this.type = requireNonNull(type, "Type should not be null");
        this.options = requireNonNull(options, "Options should not be null");

        if (replace && ifNotExists) {
            throw QueryException.error("You can't use both <OR REPLACE> and <IF NOT EXISTS> options");
        }
    }

    public String name() {
        return name.toString();
    }

    public Stream<SqlTableColumn> columns() {
        return columns.getList().stream().map(node -> (SqlTableColumn) node);
    }

    public String type() {
        return type.toString();
    }

    public Map<String, String> options() {
        return options.getList().stream().map(node -> (SqlOption) node).collect(toMap(SqlOption::key, SqlOption::value));
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    @Override
    @Nonnull
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    @Nonnull
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(name, columns, type, options);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (getReplace()) {
            writer.keyword("OR REPLACE");
        }
        writer.keyword("TABLE MAPPING");
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }

        name.unparse(writer, leftPrec, rightPrec);

        if (columns.size() > 0) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode column : columns) {
                printIndent(writer);
                column.unparse(writer, 0, 0);
            }
            writer.newlineAndIndent();
            writer.endList(frame);
        }

        writer.newlineAndIndent();
        writer.keyword("TYPE");
        type.unparse(writer, leftPrec, rightPrec);

        if (options.size() > 0) {
            writer.newlineAndIndent();
            writer.keyword("OPTIONS");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode property : options) {
                printIndent(writer);
                property.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }
    }

    private void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print(" ");
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        columns.forEach(column -> column.validate(validator, scope));

        for (int i = 0; i < columns.size(); i++) {
            SqlTableColumn column = (SqlTableColumn) columns.get(i);
            if (column.type() == null) {
                throw SqlUtil.newContextException(column.getParserPosition(), RESOURCE.missingColumnType());
            }
        }
    }
}
