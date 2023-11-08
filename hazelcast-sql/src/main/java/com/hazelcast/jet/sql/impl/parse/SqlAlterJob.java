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

import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.config.DeltaJobConfig;
import org.apache.calcite.sql.SqlAlter;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.jet.sql.impl.parse.ParserResource.RESOURCE;
import static com.hazelcast.jet.sql.impl.parse.UnparseUtil.unparseOptions;
import static java.util.Objects.requireNonNull;

public class SqlAlterJob extends SqlAlter {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("ALTER JOB", SqlKind.OTHER_DDL);

    private final SqlIdentifier name;
    private final SqlNodeList options;
    private final AlterJobOperation operation;

    private final DeltaJobConfig deltaConfig;

    public SqlAlterJob(SqlIdentifier name, SqlNodeList options, @Nullable AlterJobOperation operation, SqlParserPos pos) {
        super(pos, "JOB");

        this.name = requireNonNull(name, "Name must not be null");
        this.options = options;
        this.operation = operation;

        Preconditions.checkTrue(name.names.size() == 1, name.toString());
        Preconditions.checkTrueUnsupportedOperation(
                options == null || operation == null || operation == AlterJobOperation.RESUME,
                "Options can only be specified for RESUME operation");

        deltaConfig = options == null || options.isEmpty() ? null : new DeltaJobConfig();
    }

    public String name() {
        return name.toString();
    }

    @Nullable
    public DeltaJobConfig getDeltaConfig() {
        return deltaConfig;
    }

    /**
     * Operation is null if the command is only a change of OPTIONS. If it's null,
     * {@link #getDeltaConfig()} is not-null.
     */
    @Nullable
    public AlterJobOperation getOperation() {
        return operation;
    }

    @Override @Nonnull
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override @Nonnull
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, options);
    }

    @Override
    protected void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        name.unparse(writer, leftPrec, rightPrec);

        unparseOptions(writer, options);

        if (operation != null) {
            writer.keyword(operation.name());
        }
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (options == null) {
            return;
        }
        Set<String> optionNames = new HashSet<>();
        for (SqlNode option0 : options) {
            SqlOption option = (SqlOption) option0;
            String key = option.keyString();
            String value = option.valueString();

            if (!optionNames.add(key)) {
                throw validator.newValidationError(option, RESOURCE.duplicateOption(key));
            }

            switch (key) {
                case "snapshotIntervalMillis":
                    deltaConfig.setSnapshotIntervalMillis(ParseUtils.parseLong(validator, option));
                    break;
                case "autoScaling":
                    deltaConfig.setAutoScaling(Boolean.parseBoolean(value));
                    break;
                case "splitBrainProtectionEnabled":
                    deltaConfig.setSplitBrainProtection(Boolean.parseBoolean(value));
                    break;
                case "metricsEnabled":
                    deltaConfig.setMetricsEnabled(Boolean.parseBoolean(value));
                    break;
                case "storeMetricsAfterJobCompletion":
                    deltaConfig.setStoreMetricsAfterJobCompletion(Boolean.parseBoolean(value));
                    break;
                case "maxProcessorAccumulatedRecords":
                    deltaConfig.setMaxProcessorAccumulatedRecords(ParseUtils.parseLong(validator, option));
                    break;
                case "suspendOnFailure":
                    deltaConfig.setSuspendOnFailure(Boolean.parseBoolean(value));
                    break;
                case "processingGuarantee":
                case "initialSnapshotName":
                    throw validator.newValidationError(option.key(), RESOURCE.notSupported(key, "ALTER JOB"));
                default:
                    throw validator.newValidationError(option.key(), RESOURCE.unknownJobOption(key));
            }
        }
    }

    public enum AlterJobOperation {
        SUSPEND,
        RESUME,
        RESTART,
    }
}
