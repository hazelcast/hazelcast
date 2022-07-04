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
import org.apache.calcite.sql.SqlCreate;
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
import static java.util.Objects.requireNonNull;

public class SqlCreateSnapshot extends SqlCreate {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE SNAPSHOT", SqlKind.OTHER_DDL);

    private final SqlIdentifier snapshotName;
    private final SqlIdentifier jobName;

    public SqlCreateSnapshot(
            SqlIdentifier snapshotName,
            SqlIdentifier jobName,
            boolean replace,
            SqlParserPos pos
    ) {
        super(OPERATOR, pos, replace, false);

        this.snapshotName = requireNonNull(snapshotName, "Snapshot name must not be null");
        this.jobName = requireNonNull(jobName, "Job name must not be null");

        Preconditions.checkTrue(snapshotName.isSimple(), snapshotName.toString());
        Preconditions.checkTrue(jobName.isSimple(), jobName.toString());
    }

    public String getSnapshotName() {
        return snapshotName.toString();
    }

    public String getJobName() {
        return jobName.toString();
    }

    @Override @Nonnull
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override @Nonnull
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(snapshotName, jobName);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (getReplace()) {
            writer.keyword("OR REPLACE");
        }
        writer.keyword("SNAPSHOT");

        snapshotName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("FOR JOB");
        jobName.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (!getReplace()) {
            throw validator.newValidationError(this, RESOURCE.createSnapshotWithoutReplace());
        }
    }
}
