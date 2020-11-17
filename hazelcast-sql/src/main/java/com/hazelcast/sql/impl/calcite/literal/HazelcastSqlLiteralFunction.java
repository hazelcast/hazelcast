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

package com.hazelcast.sql.impl.calcite.literal;

import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;

public class HazelcastSqlLiteralFunction extends SqlFunction implements SqlRexConvertlet {

    private static final String NAME = "$LITERAL";

    public static final HazelcastSqlLiteralFunction INSTANCE = new HazelcastSqlLiteralFunction();

    private HazelcastSqlLiteralFunction() {
        super(
            NAME,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0,
            new OperandTypeInference(),
            null,
            SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        return true;
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        HazelcastSqlLiteral literal = call.operand(0);

        return literal.getType((HazelcastTypeFactory) validator.getTypeFactory());
    }

    @Override
    public <R> R acceptCall(SqlVisitor<R> visitor, SqlCall call) {
        return null;
    }

    @Override
    public <R> void acceptCall(
        SqlVisitor<R> visitor,
        SqlCall call,
        boolean onlyExpressions,
        SqlBasicVisitor.ArgHandler<R> argHandler
    ) {
        // No-op.
    }

    @Override
    public void validateCall(SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope) {
        // TODO: Borrow validation logic from SqlLiteral?
    }

    @Override
    public RexNode convertCall(SqlRexContext cx, SqlCall call) {
//        cx.l
//
//        cx.getRexBuilder().cast
//
//        HazelcastSqlLiteral literal = call.operand(0);
//
//        RelDataType returnType = literal.getType((HazelcastTypeFactory) cx.getTypeFactory());
//
//        cx.getRexBuilder().makeLiteral(literal.getValue(), returnType, literal.getOriginal().getTypeName());

        return null;
    }

    private static final class OperandTypeInference implements SqlOperandTypeInference {
        @Override
        public void inferOperandTypes(SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
            HazelcastSqlLiteral literal = callBinding.getCall().operand(0);

            RelDataType type = literal.getType((HazelcastTypeFactory) callBinding.getTypeFactory());

            operandTypes[0] = type;
        }
    }
}
