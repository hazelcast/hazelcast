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

package com.hazelcast.sql.impl.calcite.validate.operators;

import com.google.common.collect.ImmutableList;
import com.hazelcast.sql.impl.calcite.validate.types.ReplaceUnknownOperandTypeInference;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAny;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public class HazelcastSqlSubstringFunction extends SqlFunction {
    public HazelcastSqlSubstringFunction() {
        super(
            "SUBSTRING",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_NULLABLE_VARYING,
            new ReplaceUnknownOperandTypeInference(new SqlTypeName[] { VARCHAR, INTEGER, INTEGER }),
            null,
            SqlFunctionCategory.STRING
        );
    }

    @Override
    public String getSignatureTemplate(int operandsCount) {
        switch (operandsCount) {
            case 2:
                return "{0}({1} FROM {2})";
            case 3:
                return "{0}({1} FROM {2} FOR {3})";
            default:
                throw new AssertionError();
        }
    }

    @Override
    public String getAllowedSignatures(String opName) {
        StringBuilder ret = new StringBuilder();
        for (Ord<SqlTypeName> typeName : Ord.zip(SqlTypeName.CHAR_TYPES)) {
            if (typeName.i > 0) {
                ret.append(NL);
            }

            ret.append(
                SqlUtil.getAliasedSignature(this, opName, ImmutableList.of(typeName.e, SqlTypeName.INTEGER))
            );
            ret.append(NL);
            ret.append(
                SqlUtil.getAliasedSignature(this, opName, ImmutableList.of(typeName.e, SqlTypeName.INTEGER, SqlTypeName.INTEGER))
            );
        }
        return ret.toString();
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        List<SqlNode> operands = callBinding.operands();

        assert operands.size() == 2 || operands.size() == 3;

        SqlSingleOperandTypeChecker checker;

        if (operands.size() == 2) {
            checker = OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER);
        } else {
            checker = OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER);
        }

        checker = notAny(checker);

        return checker.checkOperandTypes(callBinding, throwOnFailure);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(2, 3);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlWriter.Frame frame = writer.startFunCall(getName());

        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.sep("FROM");
        call.operand(1).unparse(writer, leftPrec, rightPrec);

        if (call.operandCount() == 3) {
            writer.sep("FOR");
            call.operand(2).unparse(writer, leftPrec, rightPrec);
        }

        writer.endFunCall(frame);
    }
}
