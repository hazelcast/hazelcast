/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.generator;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.sql.impl.schema.JetSpecificTableFunction;
import com.hazelcast.jet.sql.impl.schema.JetTableFunctionParameter;
import com.hazelcast.jet.sql.impl.validate.operators.HazelcastOperandTypeInference;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTableStatistic;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operators.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.expression.Expression;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

public final class StreamGeneratorTableFunction extends JetSpecificTableFunction {

    private static final String SCHEMA_NAME_STREAM = "stream";
    private static final String FUNCTION_NAME = "GENERATE_STREAM";
    private static final List<JetTableFunctionParameter> PARAMETERS = singletonList(
            new JetTableFunctionParameter(0, "rate", INTEGER, TypedOperandChecker.INTEGER)
    );

    public StreamGeneratorTableFunction() {
        super(
                FUNCTION_NAME,
                PARAMETERS,
                binding -> toTable0(emptyList()).getRowType(binding.getTypeFactory()),
                new HazelcastOperandTypeInference(PARAMETERS, new ReplaceUnknownOperandTypeInference(INTEGER)),
                StreamSqlConnector.INSTANCE
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }

    @Override
    public HazelcastTable toTable(List<Expression<?>> argumentExpressions) {
        return toTable0(argumentExpressions);
    }

    private static HazelcastTable toTable0(List<Expression<?>> argumentExpressions) {
        StreamTable table = StreamSqlConnector.createTable(SCHEMA_NAME_STREAM, randomName(), argumentExpressions);
        return new HazelcastTable(table, new HazelcastTableStatistic(Integer.MAX_VALUE));
    }

    private static String randomName() {
        return SCHEMA_NAME_STREAM + "_" + UuidUtil.newUnsecureUuidString().replace('-', '_');
    }
}
