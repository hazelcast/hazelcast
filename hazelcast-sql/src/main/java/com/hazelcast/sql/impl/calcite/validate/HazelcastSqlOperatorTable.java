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

package com.hazelcast.sql.impl.calcite.validate;

import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlCase;
import com.hazelcast.sql.impl.calcite.validate.operators.datetime.HazelcastExtractFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.datetime.HazelcastToEpochMillisFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.datetime.HazelcastToTimestampTzFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.math.HazelcastAbsFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.math.HazelcastDoubleBiFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.math.HazelcastDoubleFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.math.HazelcastFloorCeilFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.math.HazelcastRandFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.math.HazelcastRoundTruncateFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.math.HazelcastSignFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.misc.HazelcastArithmeticOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.misc.HazelcastCaseOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.misc.HazelcastCastFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.misc.HazelcastCoalesceFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.misc.HazelcastDescOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.misc.HazelcastNullIfFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.misc.HazelcastUnaryOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.predicate.HazelcastAndOrPredicate;
import com.hazelcast.sql.impl.calcite.validate.operators.predicate.HazelcastBetweenOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.predicate.HazelcastComparisonPredicate;
import com.hazelcast.sql.impl.calcite.validate.operators.predicate.HazelcastInOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.predicate.HazelcastIsTrueFalseNullPredicate;
import com.hazelcast.sql.impl.calcite.validate.operators.predicate.HazelcastNotPredicate;
import com.hazelcast.sql.impl.calcite.validate.operators.string.HazelcastConcatOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.string.HazelcastLikeOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.string.HazelcastPositionFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.string.HazelcastReplaceFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.string.HazelcastStringFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.string.HazelcastSubstringFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.string.HazelcastTrimFunction;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlInfixOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.validate.HazelcastResources.RESOURCES;

/**
 * Operator table.
 * <p>
 * All supported operators and functions must be defined in this table, even if they are already defined in the
 * {@link SqlStdOperatorTable}. This is needed to ensure that we have the full control over inference and coercion
 * strategies.
 */
@SuppressWarnings({"unused", "checkstyle:ClassDataAbstractionCoupling"})
public final class HazelcastSqlOperatorTable extends ReflectiveSqlOperatorTable {

    //@formatter:off

    public static final SqlOperator CASE = HazelcastCaseOperator.INSTANCE;
    public static final SqlFunction CAST = HazelcastCastFunction.INSTANCE;

    //#region Boolean predicates.

    public static final SqlBinaryOperator AND = HazelcastAndOrPredicate.AND;
    public static final SqlBinaryOperator OR = HazelcastAndOrPredicate.OR;
    public static final SqlPrefixOperator NOT = new HazelcastNotPredicate();
    public static final SqlInfixOperator BETWEEN_ASYMMETRIC = HazelcastBetweenOperator.BETWEEN_ASYMMETRIC;
    public static final SqlInfixOperator NOT_BETWEEN_ASYMMETRIC = HazelcastBetweenOperator.NOT_BETWEEN_ASYMMETRIC;
    public static final SqlInfixOperator BETWEEN_SYMMETRIC = HazelcastBetweenOperator.BETWEEN_SYMMETRIC;
    public static final SqlInfixOperator NOT_BETWEEN_SYMMETRIC = HazelcastBetweenOperator.NOT_BETWEEN_SYMMETRIC;
    public static final SqlBinaryOperator IN = HazelcastInOperator.IN;
    public static final SqlBinaryOperator NOT_IN = HazelcastInOperator.NOT_IN;

    //#endregion

    //#region Comparison predicates.

    public static final SqlBinaryOperator EQUALS = HazelcastComparisonPredicate.EQUALS;
    public static final SqlBinaryOperator NOT_EQUALS = HazelcastComparisonPredicate.NOT_EQUALS;
    public static final SqlBinaryOperator GREATER_THAN = HazelcastComparisonPredicate.GREATER_THAN;
    public static final SqlBinaryOperator GREATER_THAN_OR_EQUAL = HazelcastComparisonPredicate.GREATER_THAN_OR_EQUAL;
    public static final SqlBinaryOperator LESS_THAN = HazelcastComparisonPredicate.LESS_THAN;
    public static final SqlBinaryOperator LESS_THAN_OR_EQUAL = HazelcastComparisonPredicate.LESS_THAN_OR_EQUAL;

    //#endregion

    //#region Binary and unary operators.

    public static final SqlOperator PLUS = HazelcastArithmeticOperator.PLUS;
    public static final SqlOperator MINUS = HazelcastArithmeticOperator.MINUS;
    public static final SqlOperator MULTIPLY = HazelcastArithmeticOperator.MULTIPLY;
    public static final SqlOperator DIVIDE = HazelcastArithmeticOperator.DIVIDE;
    public static final SqlOperator REMAINDER = HazelcastArithmeticOperator.REMAINDER;

    public static final SqlPrefixOperator UNARY_PLUS = HazelcastUnaryOperator.PLUS;
    public static final SqlPrefixOperator UNARY_MINUS = HazelcastUnaryOperator.MINUS;

    //#endregion

    //#region "IS" predicates.

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

    public static final SqlFunction POWER = new HazelcastDoubleBiFunction("POWER");
    public static final SqlFunction SQUARE = new HazelcastDoubleFunction("SQUARE");
    public static final SqlFunction SQRT = new HazelcastDoubleFunction("SQRT");
    public static final SqlFunction CBRT = new HazelcastDoubleFunction("CBRT");

    public static final SqlFunction COS = new HazelcastDoubleFunction("COS");
    public static final SqlFunction SIN = new HazelcastDoubleFunction("SIN");
    public static final SqlFunction TAN = new HazelcastDoubleFunction("TAN");
    public static final SqlFunction COT = new HazelcastDoubleFunction("COT");
    public static final SqlFunction ACOS = new HazelcastDoubleFunction("ACOS");
    public static final SqlFunction ASIN = new HazelcastDoubleFunction("ASIN");
    public static final SqlFunction ATAN = new HazelcastDoubleFunction("ATAN");
    public static final SqlFunction ATAN2 = new HazelcastDoubleBiFunction("ATAN2");
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

    public static final SqlBinaryOperator CONCAT = HazelcastConcatOperator.INSTANCE;

    public static final SqlSpecialOperator LIKE = HazelcastLikeOperator.LIKE;
    public static final SqlSpecialOperator NOT_LIKE = HazelcastLikeOperator.NOT_LIKE;

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

    public static final SqlFunction REPLACE = HazelcastReplaceFunction.INSTANCE;
    public static final SqlFunction POSITION = HazelcastPositionFunction.INSTANCE;

    public static final SqlFunction EXTRACT = HazelcastExtractFunction.INSTANCE;
    public static final SqlFunction TO_TIMESTAMP_TZ = HazelcastToTimestampTzFunction.INSTANCE;
    public static final SqlFunction TO_EPOCH_MILLIS = HazelcastToEpochMillisFunction.INSTANCE;

    public static final SqlPostfixOperator DESC = HazelcastDescOperator.DESC;

    //#endregion

    //#region null check functions

    public static final SqlFunction NULLIF = HazelcastNullIfFunction.INSTANCE;
    public static final SqlFunction COALESCE = HazelcastCoalesceFunction.INSTANCE;

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
    static final class RewriteVisitor extends SqlBasicVisitor<SqlNode> {
        private final HazelcastSqlValidator validator;

        RewriteVisitor(HazelcastSqlValidator validator) {
            this.validator = validator;
        }

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode node = nodeList.get(i);
                SqlNode rewritten = node.accept(this);
                if (rewritten != null && rewritten != node) {
                    nodeList.set(i, rewritten);
                }
            }
            return nodeList;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            call = rewriteCall(call);
            for (int i = 0; i < call.getOperandList().size(); i++) {
                SqlNode operand = call.getOperandList().get(i);
                if (operand == null) {
                    continue;
                }
                SqlNode rewritten = operand.accept(this);
                if (rewritten != null && rewritten != operand) {
                    call.setOperand(i, rewritten);
                }
            }
            return call;
        }

        private SqlCall rewriteCall(SqlCall call) {
            if (call instanceof SqlBasicCall) {
                // An alias is declared as a SqlBasicCall with "SqlKind.AS". We do not need to rewrite aliases, so skip it.
                if (call.getKind() == SqlKind.AS) {
                    return call;
                }

                SqlBasicCall basicCall = (SqlBasicCall) call;
                SqlOperator operator = basicCall.getOperator();

                List<SqlOperator> resolvedOperators = new ArrayList<>(1);

                validator.getOperatorTable().lookupOperatorOverloads(
                        operator.getNameAsId(),
                        null,
                        operator.getSyntax(),
                        resolvedOperators,
                        SqlNameMatchers.withCaseSensitive(false)
                );

                if (resolvedOperators.isEmpty()) {
                    throw functionDoesNotExist(call);
                }

                assert resolvedOperators.size() == 1;

                basicCall.setOperator(resolvedOperators.get(0));
            } else if (call instanceof SqlCase) {
                SqlCase sqlCase = (SqlCase) call;
                return new HazelcastSqlCase(sqlCase.getParserPosition(), sqlCase.getValueOperand(), sqlCase.getWhenOperands(),
                        sqlCase.getThenOperands(), sqlCase.getElseOperand());
            }
            return call;
        }

        private static SqlNodeList removeNullWithinInStatement(SqlNodeList valueList) {
            SqlNodeList list = new SqlNodeList(valueList.getParserPosition());
            for (SqlNode node : valueList.getList()) {
                if (SqlUtil.isNullLiteral(node, false)) {
                    continue;
                }
                list.add(node);
            }
            return list;
        }

        private CalciteException functionDoesNotExist(SqlCall call) {
            throw RESOURCES.functionDoesNotExist(call.getOperator().getName()).ex();
        }
    }
}
