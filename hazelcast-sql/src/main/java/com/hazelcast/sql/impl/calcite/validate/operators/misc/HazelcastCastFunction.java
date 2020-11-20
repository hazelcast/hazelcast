/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.validate.operators.misc;

import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastFunction;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

import static com.hazelcast.sql.impl.calcite.validate.operators.HazelcastReturnTypeInference.wrap;
import static org.apache.calcite.util.Static.RESOURCE;

public final class HazelcastCastFunction extends HazelcastFunction {

    public static final HazelcastCastFunction INSTANCE = new HazelcastCastFunction();

    private HazelcastCastFunction() {
        super(
            "CAST",
            SqlKind.CAST,
            wrap(new ReturnTypeInference()),
            InferTypes.FIRST_KNOWN,
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

        if (canCast(sourceType, targetType)) {
            return true;
        }

        if (throwOnFailure) {
            // TODO: Custom error message
            throw binding.newError(RESOURCE.cannotCastValue(sourceType.toString(), targetType.toString()));
        } else {
            return false;
        }
    }

    private static boolean canCast(RelDataType sourceType, RelDataType targetType) {
        QueryDataType queryFrom = SqlToQueryType.map(sourceType.getSqlTypeName());
        QueryDataType queryTo = SqlToQueryType.map(targetType.getSqlTypeName());

        return queryFrom.getConverter().canConvertTo(queryTo.getTypeFamily());
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

    private static final class ReturnTypeInference implements SqlReturnTypeInference {
        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            assert opBinding.getOperandCount() == 2;

            RelDataType sourceType = opBinding.getOperandType(0);

            // Target type must take in count nullability of the source type
            RelDataType targetType = opBinding.getTypeFactory().createTypeWithNullability(
                opBinding.getOperandType(1),
                sourceType.isNullable()
            );

            if (opBinding instanceof SqlCallBinding) {
                SqlCallBinding callBinding = (SqlCallBinding) opBinding;
                SqlNode operand0 = callBinding.operand(0);

                // dynamic parameters and null constants need their types assigned
                // to them using the type they are casted to.
                if (((operand0 instanceof SqlLiteral)
                    && (((SqlLiteral) operand0).getValue() == null))
                    || (operand0 instanceof SqlDynamicParam)) {
                    final SqlValidatorImpl validator =
                        (SqlValidatorImpl) callBinding.getValidator();
                    validator.setValidatedNodeType(operand0, targetType);
                }
            }

            return targetType;
        }
    }
}
