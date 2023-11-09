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

import com.hazelcast.jet.config.JobConfig;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.jet.config.JobConfigArguments.KEY_JOB_IS_SUSPENDABLE;
import static com.hazelcast.jet.sql.impl.parse.ParserResource.RESOURCE;
import static java.util.Objects.requireNonNull;

public class SqlAnalyzeStatement extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("ANALYZE", SqlKind.OTHER);

    private SqlNode query;
    private final SqlNodeList options;
    private final JobConfig jobConfig = new JobConfig();

    public SqlAnalyzeStatement(SqlParserPos pos, SqlNode query, SqlNodeList options) {
        super(pos);
        this.query = query;
        this.options = requireNonNull(options, "Options should not be null");
    }

    public SqlNode getQuery() {
        return query;
    }

    public void setQuery(final SqlNode query) {
        this.query = query;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return List.of(query);
    }

    public JobConfig getJobConfig() {
        return jobConfig;
    }

    @Override
    public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
        writer.keyword("ANALYZE");
        if (!options.isEmpty()) {
            UnparseUtil.unparseOptions(writer, "WITH OPTIONS", options);
        }

        query.unparse(writer, leftPrec, rightPrec);
    }

    public void validate(SqlValidator validator) {
        Set<String> optionNames = new HashSet<>();
        jobConfig.setMetricsEnabled(true);
        jobConfig.setStoreMetricsAfterJobCompletion(true);

        jobConfig.setSplitBrainProtection(false);
        jobConfig.setAutoScaling(false);
        jobConfig.setSuspendOnFailure(false);
        jobConfig.setArgument(KEY_JOB_IS_SUSPENDABLE, false);

        for (SqlNode option0 : options) {
            SqlOption option = (SqlOption) option0;
            String key = option.keyString();
            String value = option.valueString();

            if (!optionNames.add(key)) {
                throw validator.newValidationError(option, RESOURCE.duplicateOption(key));
            }

            switch (key) {
                case "processingGuarantee":
                    jobConfig.setProcessingGuarantee(ParseUtils.parseProcessingGuarantee(validator, option));
                    break;
                case "snapshotIntervalMillis":
                    jobConfig.setSnapshotIntervalMillis(ParseUtils.parseLong(validator, option));
                    break;
                case "initialSnapshotName":
                    jobConfig.setInitialSnapshotName(value);
                    break;
                case "maxProcessorAccumulatedRecords":
                    jobConfig.setMaxProcessorAccumulatedRecords(ParseUtils.parseLong(validator, option));
                    break;
                case "metricsEnabled":
                    jobConfig.setMetricsEnabled(Boolean.parseBoolean(value));
                    break;
                case "storeMetricsAfterJobCompletion":
                    jobConfig.setStoreMetricsAfterJobCompletion(Boolean.parseBoolean(value));
                    break;
                case "autoScaling":
                case "splitBrainProtectionEnabled":
                case "suspendOnFailure":
                    throw validator.newValidationError(option.key(), RESOURCE.unsupportedAnalyzeJobOption(key));
                default:
                    throw validator.newValidationError(option.key(), RESOURCE.unknownJobOption(key));
            }
        }
    }
}
