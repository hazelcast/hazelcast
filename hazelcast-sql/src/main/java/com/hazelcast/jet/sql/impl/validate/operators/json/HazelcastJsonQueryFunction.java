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
import com.hazelcast.jet.sql.impl.validate.operand.MultiTypeOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operand.OperandCheckerProgram;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.JsonFunctionOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastJsonType;
import com.hazelcast.sql.impl.QueryException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlJsonQueryEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlJsonQueryWrapperBehavior;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

@SuppressWarnings("checkstyle:MagicNumber")
public class HazelcastJsonQueryFunction extends HazelcastFunction {
    public static final HazelcastJsonQueryFunction INSTANCE = new HazelcastJsonQueryFunction();

    public HazelcastJsonQueryFunction() {
        super(
                "JSON_QUERY",
                SqlKind.OTHER_FUNCTION,
                opBinding -> HazelcastJsonType.create(true),
                new JsonFunctionOperandTypeInference(),
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    protected boolean checkOperandTypes(final HazelcastCallBinding callBinding, final boolean throwOnFailure) {
        return new OperandCheckerProgram(
                MultiTypeOperandChecker.JSON_OR_VARCHAR,
                TypedOperandChecker.VARCHAR,
                TypedOperandChecker.SYMBOL,
                TypedOperandChecker.SYMBOL,
                TypedOperandChecker.SYMBOL
        ).check(callBinding, throwOnFailure);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(5);
    }

    @Override
    public void unparse(final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
        final SqlWriter.Frame frame = writer.startFunCall(this.getName());
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.sep(",", true);
        call.operand(1).unparse(writer, leftPrec, rightPrec);

        final SqlJsonQueryWrapperBehavior wrapperBehavior = (SqlJsonQueryWrapperBehavior)
                ((SqlLiteral) call.operand(2)).getValue();

        final SqlJsonQueryEmptyOrErrorBehavior onEmpty = (SqlJsonQueryEmptyOrErrorBehavior)
                ((SqlLiteral) call.operand(3)).getValue();

        final SqlJsonQueryEmptyOrErrorBehavior onError = (SqlJsonQueryEmptyOrErrorBehavior)
                ((SqlLiteral) call.operand(4)).getValue();

        unparseWrapperBehavior(wrapperBehavior, writer);

        unparseEmptyOrErrorBehavior(onEmpty, writer);
        writer.keyword("ON EMPTY");

        unparseEmptyOrErrorBehavior(onError, writer);
        writer.keyword("ON ERROR");

        writer.endFunCall(frame);
    }

    private void unparseWrapperBehavior(final SqlJsonQueryWrapperBehavior behavior, final SqlWriter writer) {
        switch (behavior) {
            case WITHOUT_ARRAY:
                writer.keyword("WITHOUT ARRAY WRAPPER");
                break;
            case WITH_UNCONDITIONAL_ARRAY:
                writer.keyword("WITH UNCONDITIONAL ARRAY WRAPPER");
                break;
            case WITH_CONDITIONAL_ARRAY:
                writer.keyword("WITH CONDITIONAL ARRAY WRAPPER");
                break;
            default:
                throw QueryException.error("Unknown WrapperBehavior constant: " + behavior);
        }
    }

    private void unparseEmptyOrErrorBehavior(
            final SqlJsonQueryEmptyOrErrorBehavior behavior,
            final SqlWriter writer
    ) {
        switch (behavior) {
            case ERROR:
                writer.keyword("ERROR");
                break;
            case NULL:
                writer.keyword("NULL");
                break;
            case EMPTY_ARRAY:
                writer.keyword("EMPTY ARRAY");
                break;
            case EMPTY_OBJECT:
                writer.keyword("EMPTY OBJECT");
                break;
            default:
                throw QueryException.error("Unknown EmptyOrErrorBehavior constant: " + behavior);
        }
    }
}
