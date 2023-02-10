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
import com.hazelcast.jet.config.ProcessingGuarantee;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.sql.impl.parse.ParserResource.RESOURCE;
import static java.util.Objects.requireNonNull;

public class SqlCreateJob extends SqlCreate {

    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE JOB", SqlKind.OTHER_DDL);

    private final SqlIdentifier name;
    private final SqlNodeList options;
    private final SqlExtendedInsert sqlInsert;

    private Map<String, Object> parsedOptions;

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
    }

    public JobConfig jobConfig() {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(name.toString());
        processOptions(parsedOptions, jobConfig);
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

        writer.newlineAndIndent();
        writer.keyword("AS");
        sqlInsert.unparse(writer, leftPrec, rightPrec);
    }

    private void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print(" ");
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (getReplace()) {
            throw validator.newValidationError(this, RESOURCE.notSupported("OR REPLACE", "CREATE JOB"));
        }

        parsedOptions = parseOptions(options, validator, Collections.emptyList());
        validator.validate(sqlInsert);
    }

    static Map<String, Object> parseOptions(SqlNodeList options, SqlValidator validator,
                                            Collection<String> unsupportedOptions) {
        Map<String, Object> parsed = new HashMap<>();
        for (SqlNode option0 : options) {
            SqlOption option = (SqlOption) option0;
            String key = option.keyString();
            String value = option.valueString();

            if (unsupportedOptions.contains(key)) {
                throw validator.newValidationError(option, RESOURCE.notSupported(key, "CREATE JOB"));
            } else if (parsed.containsKey(key)) {
                throw validator.newValidationError(option, RESOURCE.duplicateOption(key));
            }

            switch (key) {
                case "processingGuarantee":
                    switch (value) {
                        case "exactlyOnce":
                            parsed.put(key, EXACTLY_ONCE);
                            break;
                        case "atLeastOnce":
                            parsed.put(key, AT_LEAST_ONCE);
                            break;
                        case "none":
                            parsed.put(key, NONE);
                            break;
                        default:
                            throw validator.newValidationError(option.value(),
                                    RESOURCE.processingGuaranteeBadValue(key, value));
                    }
                    break;
                case "maxProcessorAccumulatedRecords":
                case "snapshotIntervalMillis":
                    try {
                        parsed.put(key, Long.parseLong(value));
                    } catch (NumberFormatException e) {
                        throw validator.newValidationError(option.value(),
                                RESOURCE.jobOptionIncorrectNumber(key, value));
                    }
                    break;
                case "autoScaling":
                case "metricsEnabled":
                case "splitBrainProtectionEnabled":
                case "storeMetricsAfterJobCompletion":
                case "suspendOnFailure":
                    parsed.put(key, Boolean.parseBoolean(value));
                    break;
                case "initialSnapshotName":
                    parsed.put(key, value);
                    break;
                default:
                    throw validator.newValidationError(option.key(), RESOURCE.unknownJobOption(key));
            }
        }
        return parsed;
    }

    public static void processOptions(Map<String, Object> options, JobConfig jobConfig) {
        for (Entry<String, Object> option : options.entrySet()) {
            String key = option.getKey();
            Object value = option.getValue();

            switch (key) {
                case "autoScaling":
                    jobConfig.setAutoScaling((boolean) value);
                    break;
                case "initialSnapshotName":
                    jobConfig.setInitialSnapshotName((String) value);
                    break;
                case "maxProcessorAccumulatedRecords":
                    jobConfig.setMaxProcessorAccumulatedRecords((long) value);
                    break;
                case "metricsEnabled":
                    jobConfig.setMetricsEnabled((boolean) value);
                    break;
                case "processingGuarantee":
                    jobConfig.setProcessingGuarantee((ProcessingGuarantee) value);
                    break;
                case "snapshotIntervalMillis":
                    jobConfig.setSnapshotIntervalMillis((long) value);
                    break;
                case "splitBrainProtectionEnabled":
                    jobConfig.setSplitBrainProtection((boolean) value);
                    break;
                case "storeMetricsAfterJobCompletion":
                    jobConfig.setStoreMetricsAfterJobCompletion((boolean) value);
                    break;
                case "suspendOnFailure":
                    jobConfig.setSuspendOnFailure((boolean) value);
                    break;
                default:
            }
        }
    }
}
