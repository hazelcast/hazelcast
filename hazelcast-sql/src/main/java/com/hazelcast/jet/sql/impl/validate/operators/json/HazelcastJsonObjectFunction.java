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
import com.hazelcast.sql.impl.SqlErrorCode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

public class HazelcastJsonObjectFunction extends HazelcastFunction {
    public static final HazelcastJsonObjectFunction INSTANCE = new HazelcastJsonObjectFunction();

    protected HazelcastJsonObjectFunction() {
        super(
                "JSON_OBJECT",
                SqlKind.OTHER_FUNCTION,
                opBinding -> HazelcastJsonType.create(false),
                new ReplaceUnknownOperandTypeInference(SqlTypeName.ANY),
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    protected boolean checkOperandTypes(final HazelcastCallBinding callBinding, final boolean throwOnFailure) {
        // The 1st argument is always the SYMBOL: NULL ON NULL or ABSENT ON NULL
        // The rest of arguments are: key1, value2, key1, value2, ...
        // This is handled by Calcite, so we expect that the number of arguments is always odd at this point.
        if (callBinding.getOperandCount() % 2 != 1) {
            // we throw even if throwOnFailure=false because this is more like an assertion than a run-time check
            throw QueryException.error(SqlErrorCode.PARSING, "Unexpected number of arguments to JSON_OBJECT");
        }

        boolean isValid = TypedOperandChecker.SYMBOL.check(callBinding, throwOnFailure, 0);

        for (int i = 1; isValid && i < callBinding.getOperandCount(); i += 2) {
            isValid = TypedOperandChecker.VARCHAR.check(callBinding, false, i);
            if (!isValid && throwOnFailure) {
                throw QueryException.error(SqlErrorCode.PARSING, "The type of keys must be VARCHAR");
            }
        }

        return isValid;
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

        for (int i = 1; i < call.operandCount(); i += 2) {
            writer.sep(",");
            writer.keyword("KEY");
            call.operand(i).unparse(writer, leftPrec, rightPrec);
            writer.keyword("VALUE");
            call.operand(i + 1).unparse(writer, leftPrec, rightPrec);
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
