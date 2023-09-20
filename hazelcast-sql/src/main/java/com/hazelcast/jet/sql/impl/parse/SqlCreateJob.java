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
import com.hazelcast.jet.config.JobConfig;
import org.apache.calcite.sql.SqlCreate;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.sql.impl.parse.ParserResource.RESOURCE;
import static com.hazelcast.jet.sql.impl.parse.UnparseUtil.unparseOptions;
import static java.util.Objects.requireNonNull;

public class SqlCreateJob extends SqlCreate {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE JOB", SqlKind.OTHER_DDL);

    private final SqlIdentifier name;
    private final SqlNodeList options;
    private final SqlExtendedInsert sqlInsert;

    private final JobConfig jobConfig = new JobConfig();

    @SuppressWarnings("checkstyle:ExecutableStatementCount")
    public SqlCreateJob(
            SqlIdentifier name,
            SqlNodeList options,
            SqlExtendedInsert sqlInsert,
            boolean replace,
            boolean ifNotExists,
            SqlParserPos pos
    ) {
        super(OPERATOR, pos, replace, ifNotExists);

        this.name = requireNonNull(name, "Name should not be null");
        this.options = requireNonNull(options, "Options should not be null");
        this.sqlInsert = requireNonNull(sqlInsert, "A DML statement is mandatory");

        Preconditions.checkTrue(name.isSimple(), name.toString());

        jobConfig.setName(name.toString());
    }

    public JobConfig jobConfig() {
        return jobConfig;
    }

    public SqlExtendedInsert dmlStatement() {
        return sqlInsert;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    @Override @Nonnull
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override @Nonnull
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, options, sqlInsert);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE JOB");
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);

        unparseOptions(writer, options);

        writer.newlineAndIndent();
        writer.keyword("AS");
        sqlInsert.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (getReplace()) {
            throw validator.newValidationError(this, RESOURCE.notSupported("OR REPLACE", "CREATE JOB"));
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
                case "processingGuarantee":
                    switch (value) {
                        case "exactlyOnce":
                            jobConfig.setProcessingGuarantee(EXACTLY_ONCE);
                            break;
                        case "atLeastOnce":
                            jobConfig.setProcessingGuarantee(AT_LEAST_ONCE);
                            break;
                        case "none":
                            jobConfig.setProcessingGuarantee(NONE);
                            break;
                        default:
                            throw validator.newValidationError(option.value(),
                                    RESOURCE.processingGuaranteeBadValue(key, value));
                    }
                    break;
                case "snapshotIntervalMillis":
                    jobConfig.setSnapshotIntervalMillis(parseLong(validator, option));
                    break;
                case "autoScaling":
                    jobConfig.setAutoScaling(Boolean.parseBoolean(value));
                    break;
                case "splitBrainProtectionEnabled":
                    jobConfig.setSplitBrainProtection(Boolean.parseBoolean(value));
                    break;
                case "metricsEnabled":
                    jobConfig.setMetricsEnabled(Boolean.parseBoolean(value));
                    break;
                case "storeMetricsAfterJobCompletion":
                    jobConfig.setStoreMetricsAfterJobCompletion(Boolean.parseBoolean(value));
                    break;
                case "initialSnapshotName":
                    jobConfig.setInitialSnapshotName(value);
                    break;
                case "maxProcessorAccumulatedRecords":
                    jobConfig.setMaxProcessorAccumulatedRecords(parseLong(validator, option));
                    break;
                case "suspendOnFailure":
                    jobConfig.setSuspendOnFailure(Boolean.parseBoolean(value));
                    break;
                default:
                    throw validator.newValidationError(option.key(), RESOURCE.unknownJobOption(key));
            }
        }

        validator.validate(sqlInsert);
    }

    static long parseLong(SqlValidator validator, SqlOption option) {
        try {
            return Long.parseLong(option.valueString());
        } catch (NumberFormatException e) {
            throw validator.newValidationError(option.value(),
                    RESOURCE.jobOptionIncorrectNumber(option.keyString(), option.valueString()));
        }
    }
}
