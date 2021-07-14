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

import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.sql.SqlCall;
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

public class SqlMappingColumn extends SqlCall {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("COLUMN DECLARATION", SqlKind.COLUMN_DECL);

    private final SqlIdentifier name;
    private final SqlDataType type;
    private final SqlIdentifier externalName;

    public SqlMappingColumn(
            SqlIdentifier name,
            SqlDataType type,
            SqlIdentifier externalName,
            SqlParserPos pos
    ) {
        super(pos);

        this.name = requireNonNull(name, "Column name should not be null");
        this.type = requireNonNull(type, "Column type should not be null");
        this.externalName = externalName;
    }

    public String name() {
        return name.getSimple();
    }

    public QueryDataType type() {
        return type != null ? type.type() : null;
    }

    public String externalName() {
        return externalName == null ? null : externalName.toString();
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, type, externalName);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        name.unparse(writer, leftPrec, rightPrec);
        if (type != null) {
            type.unparse(writer, leftPrec, rightPrec);
        }
        if (externalName != null) {
            writer.keyword("EXTERNAL NAME");
            externalName.unparse(writer, leftPrec, rightPrec);
        }
    }
}

