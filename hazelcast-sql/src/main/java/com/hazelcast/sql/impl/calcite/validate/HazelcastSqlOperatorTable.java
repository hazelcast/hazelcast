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

import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastDoubleFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlBinaryOperator;
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
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastInferTypes.NULLABLE_OBJECT;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAllNull;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAny;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

/**
 * Custom functions and operators.
 */
@SuppressWarnings("unused")
public final class HazelcastSqlOperatorTable extends ReflectiveSqlOperatorTable {

    //@formatter:off

    public static final SqlFunction CAST = new HazelcastSqlCastFunction();

    //#region Predicates.

    public static final SqlBinaryOperator AND = new HazelcastSqlBinaryOperator(
        "AND",
        SqlKind.AND,
        SqlStdOperatorTable.AND.getLeftPrec(),
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.BOOLEAN,
        notAny(OperandTypes.BOOLEAN_BOOLEAN)
    );

    public static final SqlBinaryOperator OR = new HazelcastSqlBinaryOperator(
        "OR",
        SqlKind.OR,
        SqlStdOperatorTable.OR.getLeftPrec(),
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.BOOLEAN,
        notAny(OperandTypes.BOOLEAN_BOOLEAN)
    );

    public static final SqlPrefixOperator NOT = new SqlPrefixOperator(
        "NOT",
        SqlKind.NOT,
        SqlStdOperatorTable.NOT.getLeftPrec(),
        ReturnTypes.ARG0,
        InferTypes.BOOLEAN,
        notAny(OperandTypes.BOOLEAN)
    );

    //#endregion

    //#region Comparison operators.

    public static final SqlBinaryOperator EQUALS = new SqlBinaryOperator(
        "=",
        SqlKind.EQUALS,
        SqlStdOperatorTable.EQUALS.getLeftPrec(),
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        HazelcastInferTypes.FIRST_KNOWN,
        notAny(OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED)
    );

    public static final SqlBinaryOperator NOT_EQUALS = new SqlBinaryOperator(
        "<>",
        SqlKind.NOT_EQUALS,
        SqlStdOperatorTable.NOT_EQUALS.getLeftPrec(),
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        HazelcastInferTypes.FIRST_KNOWN,
        notAny(OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED)
    );

    public static final SqlBinaryOperator GREATER_THAN = new SqlBinaryOperator(
        ">",
        SqlKind.GREATER_THAN,
        SqlStdOperatorTable.GREATER_THAN.getLeftPrec(),
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        HazelcastInferTypes.FIRST_KNOWN,
        notAny(HazelcastOperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED)
    );

    public static final SqlBinaryOperator GREATER_THAN_OR_EQUAL = new SqlBinaryOperator(
        ">=",
        SqlKind.GREATER_THAN_OR_EQUAL,
        SqlStdOperatorTable.GREATER_THAN_OR_EQUAL.getLeftPrec(),
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        HazelcastInferTypes.FIRST_KNOWN,
        notAny(HazelcastOperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED)
    );

    public static final SqlBinaryOperator LESS_THAN = new SqlBinaryOperator(
        "<",
        SqlKind.LESS_THAN,
        SqlStdOperatorTable.LESS_THAN.getLeftPrec(),
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        HazelcastInferTypes.FIRST_KNOWN,
        notAny(HazelcastOperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED)
    );

    public static final SqlBinaryOperator LESS_THAN_OR_EQUAL = new SqlBinaryOperator(
        "<=",
        SqlKind.LESS_THAN_OR_EQUAL,
        SqlStdOperatorTable.LESS_THAN_OR_EQUAL.getLeftPrec(),
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        HazelcastInferTypes.FIRST_KNOWN,
        notAny(HazelcastOperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED)
    );

    //#endregion

    //#region Arithmetic operators.

    public static final SqlBinaryOperator PLUS = new HazelcastSqlMonotonicBinaryOperator(
        "+",
        SqlKind.PLUS,
        SqlStdOperatorTable.PLUS.getLeftPrec(),
        true,
        HazelcastReturnTypes.PLUS,
        HazelcastInferTypes.FIRST_KNOWN,
        notAllNull(notAny(OperandTypes.PLUS_OPERATOR))
    );

    public static final SqlBinaryOperator MINUS = new HazelcastSqlMonotonicBinaryOperator(
        "-",
        SqlKind.MINUS,
        SqlStdOperatorTable.MINUS.getLeftPrec(),
        true,
        HazelcastReturnTypes.MINUS,
        HazelcastInferTypes.FIRST_KNOWN,
        notAllNull(notAny(OperandTypes.MINUS_OPERATOR))
    );

    public static final SqlBinaryOperator MULTIPLY = new HazelcastSqlMonotonicBinaryOperator(
        "*",
        SqlKind.TIMES,
        SqlStdOperatorTable.MULTIPLY.getLeftPrec(),
        true,
        HazelcastReturnTypes.MULTIPLY,
        HazelcastInferTypes.FIRST_KNOWN,
        notAllNull(notAny(OperandTypes.MULTIPLY_OPERATOR))
    );

    public static final SqlBinaryOperator DIVIDE = new HazelcastSqlBinaryOperator(
        "/",
        SqlKind.DIVIDE,
        SqlStdOperatorTable.DIVIDE.getLeftPrec(),
        true,
        HazelcastReturnTypes.DIVIDE,
        HazelcastInferTypes.FIRST_KNOWN,
        notAllNull(notAny(OperandTypes.DIVISION_OPERATOR))
    );

    public static final SqlPrefixOperator UNARY_PLUS = new SqlPrefixOperator(
        "+",
        SqlKind.PLUS_PREFIX,
        SqlStdOperatorTable.UNARY_PLUS.getLeftPrec(),
        ReturnTypes.ARG0,
        InferTypes.RETURN_TYPE,
        notAllNull(notAny(OperandTypes.NUMERIC_OR_INTERVAL))
    );

    public static final SqlPrefixOperator UNARY_MINUS = new SqlPrefixOperator(
        "-",
        SqlKind.MINUS_PREFIX,
        SqlStdOperatorTable.UNARY_MINUS.getLeftPrec(),
        HazelcastReturnTypes.UNARY_MINUS,
        InferTypes.RETURN_TYPE,
        notAllNull(notAny(OperandTypes.NUMERIC_OR_INTERVAL))
    );

    //#endregion

    //#region "IS" family of predicates.

    public static final SqlPostfixOperator IS_TRUE = new SqlPostfixOperator(
        "IS TRUE",
        SqlKind.IS_TRUE,
        SqlStdOperatorTable.IS_TRUE.getLeftPrec(),
        ReturnTypes.BOOLEAN_NOT_NULL,
        InferTypes.BOOLEAN,
        notAny(OperandTypes.BOOLEAN)
    );

    public static final SqlPostfixOperator IS_NOT_TRUE = new SqlPostfixOperator(
        "IS NOT TRUE",
        SqlKind.IS_NOT_TRUE,
        SqlStdOperatorTable.IS_NOT_TRUE.getLeftPrec(),
        ReturnTypes.BOOLEAN_NOT_NULL,
        InferTypes.BOOLEAN,
        notAny(OperandTypes.BOOLEAN)
    );

    public static final SqlPostfixOperator IS_FALSE = new SqlPostfixOperator(
        "IS FALSE",
        SqlKind.IS_FALSE,
        SqlStdOperatorTable.IS_FALSE.getLeftPrec(),
        ReturnTypes.BOOLEAN_NOT_NULL,
        InferTypes.BOOLEAN,
        notAny(OperandTypes.BOOLEAN)
    );

    public static final SqlPostfixOperator IS_NOT_FALSE = new SqlPostfixOperator(
        "IS NOT FALSE",
        SqlKind.IS_NOT_FALSE,
        SqlStdOperatorTable.IS_NOT_FALSE.getLeftPrec(),
        ReturnTypes.BOOLEAN_NOT_NULL,
        InferTypes.BOOLEAN,
        notAny(OperandTypes.BOOLEAN)
    );

    public static final SqlPostfixOperator IS_NULL = new SqlPostfixOperator(
        "IS NULL",
        SqlKind.IS_NULL,
        SqlStdOperatorTable.IS_NULL.getLeftPrec(),
        ReturnTypes.BOOLEAN_NOT_NULL,
        NULLABLE_OBJECT,
        OperandTypes.ANY
    );

    public static final SqlPostfixOperator IS_NOT_NULL = new SqlPostfixOperator(
        "IS NOT NULL",
        SqlKind.IS_NOT_NULL,
        SqlStdOperatorTable.IS_NOT_NULL.getLeftPrec(),
        ReturnTypes.BOOLEAN_NOT_NULL,
        NULLABLE_OBJECT,
        OperandTypes.ANY
    );

    //#endregion

    //#region Math functions.

    public static final SqlFunction ABS = new SqlFunction(
        "ABS",
        SqlKind.OTHER_FUNCTION,
        HazelcastReturnTypes.UNARY_MINUS,
        new ReplaceUnknownOperandTypeInference(DECIMAL),
        notAny(OperandTypes.NUMERIC_OR_INTERVAL),
        SqlFunctionCategory.NUMERIC
    );

    public static final SqlFunction SIGN = new SqlFunction(
        "SIGN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        new ReplaceUnknownOperandTypeInference(DECIMAL),
        notAny(OperandTypes.NUMERIC),
        SqlFunctionCategory.NUMERIC
    );

    public static final SqlFunction RAND = new SqlFunction(
        "RAND",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE,
        HazelcastInferTypes.explicit(BIGINT),
        OperandTypes.or(OperandTypes.NILADIC, notAny(OperandTypes.NUMERIC)),
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

    public static final SqlFunction FLOOR = new HazelcastSqlFloorFunction(SqlKind.FLOOR);
    public static final SqlFunction CEIL = new HazelcastSqlFloorFunction(SqlKind.CEIL);

    public static final SqlFunction ROUND = new SqlFunction(
        "ROUND",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        new ReplaceUnknownOperandTypeInference(new SqlTypeName[] { DECIMAL, INTEGER }),
        notAny(OperandTypes.NUMERIC_OPTIONAL_INTEGER),
        SqlFunctionCategory.NUMERIC
    );

    public static final SqlFunction TRUNCATE = new SqlFunction(
        "TRUNCATE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        new ReplaceUnknownOperandTypeInference(new SqlTypeName[] { DECIMAL, INTEGER }),
        notAny(OperandTypes.NUMERIC_OPTIONAL_INTEGER),
        SqlFunctionCategory.NUMERIC
    );

    //#endregion

    //#region String functions

    public static final SqlBinaryOperator CONCAT = new SqlBinaryOperator(
        "||",
        SqlKind.OTHER,
        60,
        true,
        ReturnTypes.DYADIC_STRING_SUM_PRECISION_NULLABLE,
        new ReplaceUnknownOperandTypeInference(VARCHAR),
        notAny(OperandTypes.STRING_SAME_SAME)
    );

    public static final SqlSpecialOperator LIKE = new HazelcastSqlLikeOperator();

    public static final SqlFunction ASCII = new HazelcastSqlStringFunction(
        "ASCII",
        ReturnTypes.INTEGER_NULLABLE
    );

    public static final SqlFunction INITCAP = new HazelcastSqlStringFunction(
        "INITCAP",
        ReturnTypes.ARG0_NULLABLE
    );

    public static final SqlFunction CHAR_LENGTH = new HazelcastSqlStringFunction(
        "CHAR_LENGTH",
        ReturnTypes.INTEGER_NULLABLE
    );

    public static final SqlFunction CHARACTER_LENGTH = new HazelcastSqlStringFunction(
        "CHARACTER_LENGTH",
        ReturnTypes.INTEGER_NULLABLE
    );

    public static final SqlFunction LENGTH = new HazelcastSqlStringFunction(
        "LENGTH",
        ReturnTypes.INTEGER_NULLABLE
    );

    public static final SqlFunction LOWER = new HazelcastSqlStringFunction(
        "LOWER",
        ReturnTypes.ARG0_NULLABLE
    );

    public static final SqlFunction UPPER = new HazelcastSqlStringFunction(
        "UPPER",
        ReturnTypes.ARG0_NULLABLE
    );

    public static final SqlFunction SUBSTRING = new HazelcastSqlSubstringFunction();

    public static final SqlFunction TRIM = new HazelcastSqlTrimFunction();

    public static final SqlFunction RTRIM = new HazelcastSqlStringFunction(
        "RTRIM",
        ReturnTypes.ARG0_NULLABLE
    );

    public static final SqlFunction LTRIM = new HazelcastSqlStringFunction(
        "LTRIM",
        ReturnTypes.ARG0_NULLABLE
    );

    public static final SqlFunction BTRIM = new HazelcastSqlStringFunction(
        "BTRIM",
        ReturnTypes.ARG0_NULLABLE
    );

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
}
