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

package com.hazelcast.jet.sql.impl.validate.operators.misc;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.HazelcastResources;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import com.hazelcast.jet.sql.impl.validate.param.NoOpParameterConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.canCast;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.toHazelcastType;

public final class HazelcastCastFunction extends HazelcastFunction {

    public static final HazelcastCastFunction INSTANCE = new HazelcastCastFunction();

    private HazelcastCastFunction() {
        super(
                "CAST",
                SqlKind.CAST,
                new CastReturnTypeInference(),
                new CastOperandTypeInference(),
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        RelDataType sourceType = binding.getOperandType(0);
        RelDataType targetType = binding.getOperandType(1);

        SqlNode sourceOperand = binding.operand(0);

        if (sourceOperand.getKind() == SqlKind.DYNAMIC_PARAM) {
            int sourceParameterIndex = ((SqlDynamicParam) sourceOperand).getIndex();

            binding.getValidator().setParameterConverter(sourceParameterIndex, NoOpParameterConverter.INSTANCE);
        }

        if (canCast(sourceType, targetType)) {
            return true;
        }

        if (throwOnFailure) {
            SqlColumnType sourceType0 = toHazelcastType(sourceType).getTypeFamily().getPublicType();
            SqlColumnType targetType0 = toHazelcastType(targetType).getTypeFamily().getPublicType();

            throw binding.newError(HazelcastResources.RESOURCES.cannotCastValue(sourceType0.toString(), targetType0.toString()));
        } else {
            return false;
        }
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }

    @Override
    public String getSignatureTemplate(final int operandsCount) {
        assert operandsCount == 2;

        return "{0}({1} AS {2})";
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        assert call.operandCount() == 2;

        SqlWriter.Frame frame = writer.startFunCall(getName());

        call.operand(0).unparse(writer, 0, 0);

        writer.sep("AS");

        if (call.operand(1) instanceof SqlIntervalQualifier) {
            writer.sep("INTERVAL");
        }

        call.operand(1).unparse(writer, 0, 0);

        writer.endFunCall(frame);
    }

    private static final class CastOperandTypeInference implements SqlOperandTypeInference {
        @Override
        public void inferOperandTypes(SqlCallBinding binding, RelDataType returnType, RelDataType[] operandTypes) {
            RelDataType operandType = binding.getOperandType(0);

            if (binding.getValidator().getUnknownType().equals(operandType)) {
                operandType = binding.getTypeFactory().createSqlType(SqlTypeName.ANY);
            }

            operandTypes[0] = operandType;
            operandTypes[1] = binding.getOperandType(1);
        }
    }

    private static final class CastReturnTypeInference implements SqlReturnTypeInference {
        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            assert opBinding.getOperandCount() == 2;

            RelDataType sourceType = opBinding.getOperandType(0);

            // Target type must take in count nullability of the source type
            RelDataType targetType = opBinding.getTypeFactory().createTypeWithNullability(
                    opBinding.getOperandType(1),
                    sourceType.isNullable()
            );

            return targetType;
        }
    }
}
