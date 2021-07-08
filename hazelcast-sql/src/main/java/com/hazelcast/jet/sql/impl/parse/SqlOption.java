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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class SqlOption extends SqlCall {

    public static final SqlOperator OPERATOR =
            new SqlSpecialOperator("OPTION DECLARATION", SqlKind.OTHER);

    private final SqlNode key;
    private final SqlNode value;

    public SqlOption(SqlNode key, SqlNode value, SqlParserPos pos) {
        super(pos);

        this.key = requireNonNull(key, "Option key is missing");
        this.value = requireNonNull(value, "Option value is missing");
    }

    public SqlNode key() {
        return key;
    }

    public SqlNode value() {
        return value;
    }

    public String keyString() {
        return ((NlsString) SqlLiteral.value(key)).getValue();
    }

    // supporting just string parameters for now
    // allowing other types requires at least proper validation
    // if/when implemented, consider using it in JetTableFunctionParameter as well
    public String valueString() {
        return ((NlsString) SqlLiteral.value(value)).getValue();
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(key, value);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        key.unparse(writer, leftPrec, rightPrec);
        value.unparse(writer, leftPrec, rightPrec);
    }
}
