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

package com.hazelcast.sql.impl.calcite.validate;

import com.hazelcast.sql.impl.calcite.literal.HazelcastSqlLiteralFunction;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingManualOverride;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastDivideOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastDoubleFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastPredicateAndOr;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastPredicateComparison;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastPredicateNot;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlCastFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlFloorFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlLikeOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlMonotonicBinaryOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlStringFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlSubstringFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlTrimFunction;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastInferTypes;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastReturnTypes;
import com.hazelcast.sql.impl.calcite.validate.types.ReplaceUnknownOperandTypeInference;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.validate.HazelcastResources.RESOURCES;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastInferTypes.NULLABLE_OBJECT;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAllNull;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAny;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.wrap;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

/**
 * Operator table.
 * <p>
 * All supported operators and functions must be defined in this table, even if they are already defined in the
 * {@link SqlStdOperatorTable}. This is needed to ensure that we have the full control over inference and coercion
 * strategies.
 * <p>
 * Every operator must either have {@link HazelcastOperandTypes#wrap(SqlOperandTypeChecker)} as a top-level operand checker,
 * or implement the {@link SqlCallBindingManualOverride} interface. See {@link SqlCallBindingOverride} for more information.
 */
@SuppressWarnings({"unused", "checkstyle:ClassDataAbstractionCoupling"})
public final class HazelcastSqlOperatorTable extends ReflectiveSqlOperatorTable {

    //@formatter:off

    // TODO
    public static final SqlFunction CAST = new HazelcastSqlCastFunction();

    //#region Predicates.

    // TODO
    public static final SqlBinaryOperator AND = HazelcastPredicateAndOr.AND;
    // TODO
    public static final SqlBinaryOperator OR = HazelcastPredicateAndOr.OR;
    // TODO
    public static final SqlPrefixOperator NOT = new HazelcastPredicateNot();

    //#endregion

    //#region Comparison operators.

    public static final SqlBinaryOperator EQUALS = HazelcastPredicateComparison.EQUALS;
    public static final SqlBinaryOperator NOT_EQUALS = HazelcastPredicateComparison.NOT_EQUALS;
    public static final SqlBinaryOperator GREATER_THAN = HazelcastPredicateComparison.GREATER_THAN;
    public static final SqlBinaryOperator GREATER_THAN_OR_EQUAL = HazelcastPredicateComparison.GREATER_THAN_OR_EQUAL;
    public static final SqlBinaryOperator LESS_THAN = HazelcastPredicateComparison.LESS_THAN;
    public static final SqlBinaryOperator LESS_THAN_OR_EQUAL = HazelcastPredicateComparison.LESS_THAN_OR_EQUAL;

    //#endregion

    //#region Arithmetic operators.

    // TODO
    public static final SqlBinaryOperator PLUS = new HazelcastSqlMonotonicBinaryOperator(
        "+",
        SqlKind.PLUS,
        SqlStdOperatorTable.PLUS.getLeftPrec(),
        true,
        HazelcastReturnTypes.PLUS,
        HazelcastInferTypes.FIRST_KNOWN,
        wrap(notAllNull(notAny(OperandTypes.PLUS_OPERATOR)))
    );

    // TODO
    public static final SqlBinaryOperator MINUS = new HazelcastSqlMonotonicBinaryOperator(
        "-",
        SqlKind.MINUS,
        SqlStdOperatorTable.MINUS.getLeftPrec(),
        true,
        HazelcastReturnTypes.MINUS,
        HazelcastInferTypes.FIRST_KNOWN,
        wrap(notAllNull(notAny(OperandTypes.MINUS_OPERATOR)))
    );

    // TODO
    public static final SqlBinaryOperator MULTIPLY = new HazelcastSqlMonotonicBinaryOperator(
        "*",
        SqlKind.TIMES,
        SqlStdOperatorTable.MULTIPLY.getLeftPrec(),
        true,
        HazelcastReturnTypes.MULTIPLY,
        HazelcastInferTypes.FIRST_KNOWN,
        wrap(notAllNull(notAny(OperandTypes.MULTIPLY_OPERATOR)))
    );

    // TODO
    public static final SqlBinaryOperator DIVIDE = new HazelcastDivideOperator();

    // TODO
    public static final SqlPrefixOperator UNARY_PLUS = new SqlPrefixOperator(
        "+",
        SqlKind.PLUS_PREFIX,
        SqlStdOperatorTable.UNARY_PLUS.getLeftPrec(),
        ReturnTypes.ARG0,
        InferTypes.RETURN_TYPE,
        wrap(notAllNull(notAny(OperandTypes.NUMERIC_OR_INTERVAL)))
    );

    // TODO
    public static final SqlPrefixOperator UNARY_MINUS = new SqlPrefixOperator(
        "-",
        SqlKind.MINUS_PREFIX,
        SqlStdOperatorTable.UNARY_MINUS.getLeftPrec(),
        HazelcastReturnTypes.UNARY_MINUS,
        InferTypes.RETURN_TYPE,
        wrap(notAllNull(notAny(OperandTypes.NUMERIC_OR_INTERVAL)))
    );

    //#endregion

    //#region "IS" family of predicates.

    // TODO
    public static final SqlPostfixOperator IS_TRUE = new SqlPostfixOperator(
        "IS TRUE",
        SqlKind.IS_TRUE,
        SqlStdOperatorTable.IS_TRUE.getLeftPrec(),
        ReturnTypes.BOOLEAN_NOT_NULL,
        InferTypes.BOOLEAN,
        wrap(notAny(OperandTypes.BOOLEAN))
    );

    // TODO
    public static final SqlPostfixOperator IS_NOT_TRUE = new SqlPostfixOperator(
        "IS NOT TRUE",
        SqlKind.IS_NOT_TRUE,
        SqlStdOperatorTable.IS_NOT_TRUE.getLeftPrec(),
        ReturnTypes.BOOLEAN_NOT_NULL,
        InferTypes.BOOLEAN,
        wrap(notAny(OperandTypes.BOOLEAN))
    );

    // TODO
    public static final SqlPostfixOperator IS_FALSE = new SqlPostfixOperator(
        "IS FALSE",
        SqlKind.IS_FALSE,
        SqlStdOperatorTable.IS_FALSE.getLeftPrec(),
        ReturnTypes.BOOLEAN_NOT_NULL,
        InferTypes.BOOLEAN,
        wrap(notAny(OperandTypes.BOOLEAN))
    );

    // TODO
    public static final SqlPostfixOperator IS_NOT_FALSE = new SqlPostfixOperator(
        "IS NOT FALSE",
        SqlKind.IS_NOT_FALSE,
        SqlStdOperatorTable.IS_NOT_FALSE.getLeftPrec(),
        ReturnTypes.BOOLEAN_NOT_NULL,
        InferTypes.BOOLEAN,
        wrap(notAny(OperandTypes.BOOLEAN))
    );

    // TODO
    public static final SqlPostfixOperator IS_NULL = new SqlPostfixOperator(
        "IS NULL",
        SqlKind.IS_NULL,
        SqlStdOperatorTable.IS_NULL.getLeftPrec(),
        ReturnTypes.BOOLEAN_NOT_NULL,
        NULLABLE_OBJECT,
        wrap(OperandTypes.ANY)
    );

    // TODO
    public static final SqlPostfixOperator IS_NOT_NULL = new SqlPostfixOperator(
        "IS NOT NULL",
        SqlKind.IS_NOT_NULL,
        SqlStdOperatorTable.IS_NOT_NULL.getLeftPrec(),
        ReturnTypes.BOOLEAN_NOT_NULL,
        NULLABLE_OBJECT,
        wrap(OperandTypes.ANY)
    );

    //#endregion

    //#region Math functions.

    // TODO
    public static final SqlFunction ABS = new SqlFunction(
        "ABS",
        SqlKind.OTHER_FUNCTION,
        HazelcastReturnTypes.UNARY_MINUS,
        new ReplaceUnknownOperandTypeInference(DECIMAL),
        wrap(notAny(OperandTypes.NUMERIC_OR_INTERVAL)),
        SqlFunctionCategory.NUMERIC
    );

    // TODO
    public static final SqlFunction SIGN = new SqlFunction(
        "SIGN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        new ReplaceUnknownOperandTypeInference(DECIMAL),
        wrap(notAny(OperandTypes.NUMERIC)),
        SqlFunctionCategory.NUMERIC
    );

    // TODO
    public static final SqlFunction RAND = new SqlFunction(
        "RAND",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE,
        HazelcastInferTypes.explicitSingle(BIGINT),
        wrap(OperandTypes.or(OperandTypes.NILADIC, notAny(OperandTypes.NUMERIC))),
        SqlFunctionCategory.NUMERIC
    );

    public static final SqlFunction COS = new HazelcastDoubleFunction("COS");
    public static final SqlFunction SIN = new HazelcastDoubleFunction("SIN");
    public static final SqlFunction TAN = new HazelcastDoubleFunction("TAN");
    public static final SqlFunction COT = new HazelcastDoubleFunction("COT");
    public static final SqlFunction ACOS = new HazelcastDoubleFunction("ACOS");
    public static final SqlFunction ASIN = new HazelcastDoubleFunction("ASIN");
    public static final SqlFunction ATAN = new HazelcastDoubleFunction("ATAN");
    public static final SqlFunction EXP = new HazelcastDoubleFunction("EXP");
    public static final SqlFunction LN = new HazelcastDoubleFunction("LN");
    public static final SqlFunction LOG10 = new HazelcastDoubleFunction("LOG10");
    public static final SqlFunction DEGREES = new HazelcastDoubleFunction("DEGREES");
    public static final SqlFunction RADIANS = new HazelcastDoubleFunction("RADIANS");

    // TODO
    public static final SqlFunction FLOOR = new HazelcastSqlFloorFunction(SqlKind.FLOOR);
    // TODO
    public static final SqlFunction CEIL = new HazelcastSqlFloorFunction(SqlKind.CEIL);

    // TODO
    public static final SqlFunction ROUND = new SqlFunction(
        "ROUND",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        new ReplaceUnknownOperandTypeInference(new SqlTypeName[] { DECIMAL, INTEGER }),
        wrap(notAny(OperandTypes.NUMERIC_OPTIONAL_INTEGER)),
        SqlFunctionCategory.NUMERIC
    );

    // TODO
    public static final SqlFunction TRUNCATE = new SqlFunction(
        "TRUNCATE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        new ReplaceUnknownOperandTypeInference(new SqlTypeName[] { DECIMAL, INTEGER }),
        wrap(notAny(OperandTypes.NUMERIC_OPTIONAL_INTEGER)),
        SqlFunctionCategory.NUMERIC
    );

    //#endregion

    //#region String functions

    // TODO
    public static final SqlBinaryOperator CONCAT = new SqlBinaryOperator(
        "||",
        SqlKind.OTHER,
        60,
        true,
        ReturnTypes.DYADIC_STRING_SUM_PRECISION_NULLABLE,
        new ReplaceUnknownOperandTypeInference(VARCHAR),
        wrap(notAny(OperandTypes.STRING_SAME_SAME))
    );

    // TODO
    public static final SqlSpecialOperator LIKE = new HazelcastSqlLikeOperator();

    public static final SqlFunction ASCII = HazelcastSqlStringFunction.withIntegerReturn("ASCII");
    public static final SqlFunction INITCAP = HazelcastSqlStringFunction.withStringReturn("INITCAP");

    public static final SqlFunction CHAR_LENGTH = HazelcastSqlStringFunction.withIntegerReturn("CHAR_LENGTH");
    public static final SqlFunction CHARACTER_LENGTH = HazelcastSqlStringFunction.withIntegerReturn("CHARACTER_LENGTH");
    public static final SqlFunction LENGTH = HazelcastSqlStringFunction.withIntegerReturn("LENGTH");

    public static final SqlFunction LOWER = HazelcastSqlStringFunction.withStringReturn("LOWER");
    public static final SqlFunction UPPER = HazelcastSqlStringFunction.withStringReturn("UPPER");

    // TODO
    public static final SqlFunction SUBSTRING = new HazelcastSqlSubstringFunction();

    // TODO
    public static final SqlFunction TRIM = new HazelcastSqlTrimFunction();

    public static final SqlFunction RTRIM = HazelcastSqlStringFunction.withStringReturn("RTRIM");
    public static final SqlFunction LTRIM = HazelcastSqlStringFunction.withStringReturn("LTRIM");
    public static final SqlFunction BTRIM = HazelcastSqlStringFunction.withStringReturn("BTRIM");

    //#endregion

    //@formatter:on

    private static final HazelcastSqlOperatorTable INSTANCE = new HazelcastSqlOperatorTable();

    static {
        INSTANCE.init();
    }

    private HazelcastSqlOperatorTable() {
        // No-op.
    }

    public static HazelcastSqlOperatorTable instance() {
        return INSTANCE;
    }

    /**
     * Visitor that rewrites Calcite operators with operators from this table.
     */
    public static class RewriteVisitor extends SqlBasicVisitor<Void> {

        private final HazelcastSqlValidator validator;

        public RewriteVisitor(HazelcastSqlValidator validator) {
            this.validator = validator;
        }

        @Override
        public Void visit(SqlCall call) {
            // Do not rewrite literals
            if (call.getOperator() == HazelcastSqlLiteralFunction.INSTANCE) {
                return null;
            }

            rewriteCall(call);

            return super.visit(call);
        }

        private void rewriteCall(SqlCall call) {
            if (call instanceof SqlBasicCall) {
                // An alias is declared as a SqlBasicCall with "SqlKind.AS". We do not need to rewrite aliases, so skip it.
                if (call.getKind() == SqlKind.AS) {
                    return;
                }

                SqlBasicCall basicCall = (SqlBasicCall) call;
                SqlOperator operator = basicCall.getOperator();

                List<SqlOperator> resolvedOperators = new ArrayList<>(1);

                HazelcastSqlOperatorTable.instance().lookupOperatorOverloads(
                    operator.getNameAsId(),
                    null,
                    operator.getSyntax(),
                    resolvedOperators,
                    validator.getCatalogReader().nameMatcher()
                );

                if (resolvedOperators.isEmpty()) {
                    throw functionDoesNotExist(call);
                }

                assert resolvedOperators.size() == 1;

                basicCall.setOperator(resolvedOperators.get(0));
            } else if (call instanceof SqlCase) {
                // TODO: Support CASE
                throw functionDoesNotExist(call);
            }
        }

        private CalciteException functionDoesNotExist(SqlCall call) {
            throw RESOURCES.functionDoesNotExist(call.getOperator().getName()).ex();
        }
    }
}
