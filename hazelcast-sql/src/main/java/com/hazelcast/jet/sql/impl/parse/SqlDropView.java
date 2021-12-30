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

public class SqlDropView extends SqlDrop {
    private static final SqlSpecialOperator DROP_VIEW =
            new SqlSpecialOperator("DROP VIEW", SqlKind.DROP_VIEW);

    private final SqlIdentifier viewName;

    public SqlDropView(SqlIdentifier name, boolean ifExists, SqlParserPos pos) {
        super(DROP_VIEW, pos, ifExists);
        this.viewName = requireNonNull(name, "View name should not be null");
    }

    public boolean ifExists() {
        return ifExists;
    }

    public String viewName() {
        return viewName.toString();
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return DROP_VIEW;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(viewName);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP VIEW");
        if (ifExists) {
            writer.keyword("IF EXISTS");
        }
        viewName.unparse(writer, leftPrec, rightPrec);
    }
}
