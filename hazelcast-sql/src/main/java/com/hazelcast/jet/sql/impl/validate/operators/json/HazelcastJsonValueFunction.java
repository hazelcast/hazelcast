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

package com.hazelcast.jet.sql.impl.validate.operators.json;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.JsonFunctionOperandTypeInference;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlJsonValueReturning;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

@SuppressWarnings("checkstyle:MagicNumber")
public class HazelcastJsonValueFunction extends HazelcastFunction {
    public static final HazelcastJsonValueFunction INSTANCE = new HazelcastJsonValueFunction();

    public HazelcastJsonValueFunction() {
        super(
                "JSON_VALUE",
                SqlKind.OTHER_FUNCTION,
                new JsonValueFunctionReturnTypeInference(),
                new JsonFunctionOperandTypeInference(),
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    protected boolean checkOperandTypes(final HazelcastCallBinding callBinding, final boolean throwOnFailure) {
        return JsonFunctionUtil.checkJsonOperandType(callBinding, throwOnFailure, 0)
                && TypedOperandChecker.VARCHAR.check(callBinding, throwOnFailure, 1);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.from(2);
    }

    private static final class JsonValueFunctionReturnTypeInference implements SqlReturnTypeInference {

        @Override
        public RelDataType inferReturnType(final SqlOperatorBinding binding) {
            for (int i = 2; i < binding.getOperandCount(); i += 2) {
                if (!binding.getOperandType(i).getSqlTypeName().equals(SqlTypeName.SYMBOL)) {
                    continue;
                }
                final Object value = ((SqlLiteral) ((SqlCallBinding) binding).getCall().operand(i)).getValue();
                if (!(value instanceof SqlJsonValueReturning)) {
                    continue;
                }

                return binding.getOperandType(i + 1);
            }

            return binding.getTypeFactory().createSqlType(SqlTypeName.ANY);
        }
    }
}
