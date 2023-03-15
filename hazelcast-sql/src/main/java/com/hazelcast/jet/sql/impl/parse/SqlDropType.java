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

import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.jet.sql.impl.parse.ParserResource.RESOURCE;
import static com.hazelcast.jet.sql.impl.validate.ValidationUtil.isCatalogObjectNameValid;
import static java.util.Objects.requireNonNull;

public class SqlDropType extends SqlDrop {
    private static final SqlSpecialOperator DROP_TYPE =
            new SqlSpecialOperator("DROP TYPE", SqlKind.DROP_TYPE);

    private final SqlIdentifier name;

    public SqlDropType(SqlIdentifier name, boolean ifExists, SqlParserPos pos) {
        super(DROP_TYPE, pos, ifExists);
        this.name = requireNonNull(name, "Type name should not be null");
    }

    public boolean ifExists() {
        return ifExists;
    }

    public String typeName() {
        return name.names.get(name.names.size() - 1);
    }

    @Nonnull @Override
    public SqlOperator getOperator() {
        return DROP_TYPE;
    }

    @Nonnull @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name);
    }

    @Override
    public void validate(final SqlValidator validator, final SqlValidatorScope scope) {
        if (!isCatalogObjectNameValid(name)) {
            throw validator.newValidationError(name, RESOURCE.droppedTypeDoesNotExist(name.toString()));
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP TYPE");
        if (ifExists) {
            writer.keyword("IF EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
    }
}
