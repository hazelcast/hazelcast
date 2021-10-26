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

package com.hazelcast.jet.sql.impl.connector.generator;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.sql.impl.schema.HazelcastSpecificTableFunction;
import com.hazelcast.jet.sql.impl.schema.HazelcastSqlOperandMetadata;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.schema.HazelcastTableFunctionParameter;
import com.hazelcast.jet.sql.impl.schema.HazelcastTableStatistic;
import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.HazelcastOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.expression.Expression;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

public final class SeriesGeneratorTableFunction extends HazelcastSpecificTableFunction {

    private static final String SCHEMA_NAME_SERIES = "series";
    private static final String FUNCTION_NAME = "GENERATE_SERIES";
    private static final List<HazelcastTableFunctionParameter> PARAMETERS = asList(
            new HazelcastTableFunctionParameter(0, "start", SqlTypeName.INTEGER, false, TypedOperandChecker.INTEGER),
            new HazelcastTableFunctionParameter(1, "stop", SqlTypeName.INTEGER, false, TypedOperandChecker.INTEGER),
            new HazelcastTableFunctionParameter(2, "step", SqlTypeName.INTEGER, true, TypedOperandChecker.INTEGER)
    );

    public SeriesGeneratorTableFunction() {
        super(
                FUNCTION_NAME,
                SeriesOperandMetadata.INSTANCE,
                binding -> toTable0(emptyList()).getRowType(binding.getTypeFactory()),
                SeriesSqlConnector.INSTANCE
        );
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

    private static final class SeriesOperandMetadata extends HazelcastSqlOperandMetadata {

        private static final SeriesOperandMetadata INSTANCE = new SeriesOperandMetadata();

        private SeriesOperandMetadata() {
            super(
                    PARAMETERS,
                    new HazelcastOperandTypeInference(PARAMETERS, new ReplaceUnknownOperandTypeInference(INTEGER))
            );
        }

        @Override
        protected boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
            return true;
        }
    }
}
