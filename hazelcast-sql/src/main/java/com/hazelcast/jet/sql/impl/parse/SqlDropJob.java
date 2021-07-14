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

import com.hazelcast.internal.util.Preconditions;
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
import javax.annotation.Nullable;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class SqlDropJob extends SqlDrop {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("DROP JOB", SqlKind.OTHER_DDL);

    private final SqlIdentifier name;
    private final SqlIdentifier withSnapshotName;

    public SqlDropJob(SqlIdentifier name, boolean ifExists, SqlIdentifier withSnapshotName, SqlParserPos pos) {
        super(OPERATOR, pos, ifExists);

        this.name = requireNonNull(name, "Name should not be null");
        this.withSnapshotName = withSnapshotName;

        Preconditions.checkTrue(name.names.size() == 1, name.toString());
        Preconditions.checkTrue(withSnapshotName == null || withSnapshotName.names.size() == 1, "" + withSnapshotName);
    }

    public String name() {
        return name.toString();
    }

    public boolean ifExists() {
        return ifExists;
    }

    @Nullable
    public String withSnapshotName() {
        return withSnapshotName != null ? withSnapshotName.toString() : null;
    }

    @Override @Nonnull
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override @Nonnull
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP JOB");
        if (ifExists) {
            writer.keyword("IF EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
    }
}
