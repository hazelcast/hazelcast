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

import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class SqlDropExternalTable extends SqlDrop {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("DROP EXTERNAL TABLE", SqlKind.DROP_TABLE);

    private final SqlIdentifier name;

    public SqlDropExternalTable(SqlIdentifier name, boolean ifExists, SqlParserPos pos) {
        super(OPERATOR, pos, ifExists);
        this.name = requireNonNull(name, "Name should not be null");
    }

    public String name() {
        return name.toString();
    }

    public boolean ifExists() {
        return ifExists;
    }

    @Override
    @Nonnull
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    @Nonnull
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP");
        writer.keyword("EXTERNAL");
        writer.keyword("TABLE");

        if (ifExists) {
            writer.keyword("IF");
            writer.keyword("EXISTS");
        }

        name.unparse(writer, leftPrec, rightPrec);
    }
}
