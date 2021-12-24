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
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastJsonType;
import com.hazelcast.sql.impl.QueryException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

public class HazelcastJsonArrayFunction extends HazelcastFunction {
    public static final HazelcastJsonArrayFunction INSTANCE = new HazelcastJsonArrayFunction();

    protected HazelcastJsonArrayFunction() {
        super(
                "JSON_ARRAY",
                SqlKind.OTHER_FUNCTION,
                opBinding -> HazelcastJsonType.create(false),
                new ReplaceUnknownOperandTypeInference(SqlTypeName.ANY),
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    protected boolean checkOperandTypes(final HazelcastCallBinding callBinding, final boolean throwOnFailure) {
        return TypedOperandChecker.SYMBOL.check(callBinding, throwOnFailure, 0);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.from(1);
    }

    @Override
    public void unparse(final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
        final SqlWriter.Frame frame = writer.startFunCall(this.getName());
        if (call.operandCount() == 1) {
            writer.endFunCall(frame);
            return;
        }

        for (int i = 1; i < call.operandCount(); i++) {
            writer.sep(",");
            call.operand(i).unparse(writer, leftPrec, rightPrec);
        }

        final SqlJsonConstructorNullClause nullClause = (SqlJsonConstructorNullClause)
                ((SqlLiteral) call.operand(0)).getValue();
        switch (nullClause) {
            case ABSENT_ON_NULL:
                writer.keyword("ABSENT ON NULL");
                break;
            case NULL_ON_NULL:
                writer.keyword("NULL ON NULL");
                break;
            default:
                throw QueryException.error("Unknown SqlJsonConstructorNullClause constant: " + nullClause);
        }

        writer.endFunCall(frame);
    }
}
