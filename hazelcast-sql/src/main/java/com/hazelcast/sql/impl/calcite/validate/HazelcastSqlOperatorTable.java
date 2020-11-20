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

import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingManualOverride;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastDivideOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlCastFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlMonotonicBinaryOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.math.HazelcastAbsFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.math.HazelcastDoubleFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.math.HazelcastFloorCeilFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.math.HazelcastRandFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.math.HazelcastRoundTruncateFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.math.HazelcastSignFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.predicate.HazelcastAndOrPredicate;
import com.hazelcast.sql.impl.calcite.validate.operators.predicate.HazelcastComparisonPredicate;
import com.hazelcast.sql.impl.calcite.validate.operators.predicate.HazelcastIsTrueFalseNullPredicate;
import com.hazelcast.sql.impl.calcite.validate.operators.predicate.HazelcastNotPredicate;
import com.hazelcast.sql.impl.calcite.validate.operators.string.HazelcastConcatFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.string.HazelcastLikeOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.string.HazelcastStringFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.string.HazelcastSubstringFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.string.HazelcastTrimFunction;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastInferTypes;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastReturnTypes;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
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
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.validate.HazelcastResources.RESOURCES;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAllNull;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAny;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.wrap;

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

    public static final SqlBinaryOperator AND = HazelcastAndOrPredicate.AND;
    public static final SqlBinaryOperator OR = HazelcastAndOrPredicate.OR;
    public static final SqlPrefixOperator NOT = new HazelcastNotPredicate();

    //#endregion

    //#region Comparison operators.

    public static final SqlBinaryOperator EQUALS = HazelcastComparisonPredicate.EQUALS;
    public static final SqlBinaryOperator NOT_EQUALS = HazelcastComparisonPredicate.NOT_EQUALS;
    public static final SqlBinaryOperator GREATER_THAN = HazelcastComparisonPredicate.GREATER_THAN;
    public static final SqlBinaryOperator GREATER_THAN_OR_EQUAL = HazelcastComparisonPredicate.GREATER_THAN_OR_EQUAL;
    public static final SqlBinaryOperator LESS_THAN = HazelcastComparisonPredicate.LESS_THAN;
    public static final SqlBinaryOperator LESS_THAN_OR_EQUAL = HazelcastComparisonPredicate.LESS_THAN_OR_EQUAL;

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

    public static final SqlPostfixOperator IS_TRUE = HazelcastIsTrueFalseNullPredicate.IS_TRUE;
    public static final SqlPostfixOperator IS_NOT_TRUE = HazelcastIsTrueFalseNullPredicate.IS_NOT_TRUE;
    public static final SqlPostfixOperator IS_FALSE = HazelcastIsTrueFalseNullPredicate.IS_FALSE;
    public static final SqlPostfixOperator IS_NOT_FALSE = HazelcastIsTrueFalseNullPredicate.IS_NOT_FALSE;
    public static final SqlPostfixOperator IS_NULL = HazelcastIsTrueFalseNullPredicate.IS_NULL;
    public static final SqlPostfixOperator IS_NOT_NULL = HazelcastIsTrueFalseNullPredicate.IS_NOT_NULL;

    //#endregion

    //#region Math functions.

    public static final SqlFunction ABS = HazelcastAbsFunction.INSTANCE;

    public static final SqlFunction SIGN = HazelcastSignFunction.INSTANCE;
    public static final SqlFunction RAND = HazelcastRandFunction.INSTANCE;

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

    public static final SqlFunction FLOOR = HazelcastFloorCeilFunction.FLOOR;
    public static final SqlFunction CEIL = HazelcastFloorCeilFunction.CEIL;

    public static final SqlFunction ROUND = HazelcastRoundTruncateFunction.ROUND;
    public static final SqlFunction TRUNCATE = HazelcastRoundTruncateFunction.TRUNCATE;

    //#endregion

    //#region String functions

    public static final SqlBinaryOperator CONCAT = HazelcastConcatFunction.INSTANCE;

    public static final SqlSpecialOperator LIKE = HazelcastLikeOperator.INSTANCE;

    public static final SqlFunction SUBSTRING = HazelcastSubstringFunction.INSTANCE;

    public static final SqlFunction TRIM = HazelcastTrimFunction.INSTANCE;

    public static final SqlFunction RTRIM = HazelcastStringFunction.RTRIM;
    public static final SqlFunction LTRIM = HazelcastStringFunction.LTRIM;
    public static final SqlFunction BTRIM = HazelcastStringFunction.BTRIM;

    public static final SqlFunction ASCII = HazelcastStringFunction.ASCII;
    public static final SqlFunction INITCAP = HazelcastStringFunction.INITCAP;

    public static final SqlFunction CHAR_LENGTH = HazelcastStringFunction.CHAR_LENGTH;
    public static final SqlFunction CHARACTER_LENGTH = HazelcastStringFunction.CHARACTER_LENGTH;
    public static final SqlFunction LENGTH = HazelcastStringFunction.LENGTH;

    public static final SqlFunction LOWER = HazelcastStringFunction.LOWER;
    public static final SqlFunction UPPER = HazelcastStringFunction.UPPER;

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
                throw functionDoesNotExist(call);
            }
        }

        private CalciteException functionDoesNotExist(SqlCall call) {
            throw RESOURCES.functionDoesNotExist(call.getOperator().getName()).ex();
        }
    }
}
