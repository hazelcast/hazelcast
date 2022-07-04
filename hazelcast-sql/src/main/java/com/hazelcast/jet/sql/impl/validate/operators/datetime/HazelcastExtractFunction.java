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

package com.hazelcast.jet.sql.impl.validate.operators.datetime;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.param.NoOpParameterConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

public final class HazelcastExtractFunction extends HazelcastFunction {
    public static final HazelcastExtractFunction INSTANCE = new HazelcastExtractFunction();

    private HazelcastExtractFunction() {
        super(
                "EXTRACT",
                SqlKind.EXTRACT,
                ReturnTypes.DOUBLE_NULLABLE,
                new ReplaceUnknownOperandTypeInference(SqlTypeName.ANY),
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        SqlNode sourceOperand = callBinding.operand(1);

        RelDataType fieldType = callBinding.getOperandType(0);
        RelDataType sourceType = callBinding.getOperandType(1);

        if (sourceOperand.getKind() == SqlKind.DYNAMIC_PARAM) {
            // Set parameter type
            callBinding.getValidator().setValidatedNodeType(sourceOperand, fieldType);

            int parameterIndex = ((SqlDynamicParam) sourceOperand).getIndex();
            callBinding.getValidator().setParameterConverter(parameterIndex, NoOpParameterConverter.INSTANCE);
        }

        if (!isFieldValid(fieldType) || !isSourceValid(sourceType)) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }
        return true;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlWriter.Frame frame = writer.startFunCall(getName());

        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.sep("FROM");
        call.operand(1).unparse(writer, leftPrec, rightPrec);

        writer.endFunCall(frame);
    }

    private static boolean isSourceValid(RelDataType sourceType) {
        switch (sourceType.getSqlTypeName()) {
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case DATE:
            case TIME:
            case ANY:
                return true;
            default:
                return false;
        }
    }

    private static boolean isFieldValid(RelDataType fieldType) {
        switch (fieldType.getSqlTypeName()) {
            case INTERVAL_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_HOUR:
            case INTERVAL_DAY:
            case INTERVAL_MONTH:
            case INTERVAL_YEAR:
                return true;
            default:
                return false;
        }
    }
}
