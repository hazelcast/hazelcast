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
import com.hazelcast.jet.sql.impl.validate.ValidationUtil;
import com.hazelcast.jet.sql.impl.validate.operand.NamedOperandCheckerProgram;
import com.hazelcast.jet.sql.impl.validate.operators.HazelcastOperandTypeInference;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTableStatistic;
import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operand.OperandCheckerProgram;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operators.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.expression.Expression;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

public final class SeriesGeneratorTableFunction extends JetSpecificTableFunction {

    private static final String SCHEMA_NAME_SERIES = "series";
    private static final String FUNCTION_NAME = "GENERATE_SERIES";
    private static final List<String> PARAM_NAMES = asList("start", "stop", "step");

    public SeriesGeneratorTableFunction() {
        super(
                FUNCTION_NAME,
                binding -> toTable0(emptyList()).getRowType(binding.getTypeFactory()),
                new HazelcastOperandTypeInference(
                        new SqlTypeName[]{INTEGER, INTEGER, INTEGER},
                        new ReplaceUnknownOperandTypeInference(INTEGER)
                ),
                SeriesSqlConnector.INSTANCE
        );
    }

    @Override
    public List<String> getParamNames() {
        return PARAM_NAMES;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(2, 3);
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        if (ValidationUtil.hasAssignment(binding.getCall())) {
            return new NamedOperandCheckerProgram(
                    TypedOperandChecker.INTEGER,
                    TypedOperandChecker.INTEGER,
                    TypedOperandChecker.INTEGER
            ).check(binding, throwOnFailure);
        } else if (binding.getOperandCount() == 3) {
            return new OperandCheckerProgram(
                    TypedOperandChecker.INTEGER,
                    TypedOperandChecker.INTEGER,
                    TypedOperandChecker.INTEGER
            ).check(binding, throwOnFailure);
        } else {
            assert binding.getOperandCount() == 2;

            return new OperandCheckerProgram(
                    TypedOperandChecker.INTEGER,
                    TypedOperandChecker.INTEGER
            ).check(binding, throwOnFailure);
        }
    }

    @Override
    public HazelcastTable toTable(List<Expression<?>> argumentExpressions) {
        return toTable0(argumentExpressions);
    }

    private static HazelcastTable toTable0(List<Expression<?>> argumentExpressions) {
        SeriesTable table = SeriesSqlConnector.createTable(SCHEMA_NAME_SERIES, randomName(), argumentExpressions);
        return new HazelcastTable(table, new HazelcastTableStatistic(0));
    }

    private static String randomName() {
        return SCHEMA_NAME_SERIES + "_" + UuidUtil.newUnsecureUuidString().replace('-', '_');
    }
}
