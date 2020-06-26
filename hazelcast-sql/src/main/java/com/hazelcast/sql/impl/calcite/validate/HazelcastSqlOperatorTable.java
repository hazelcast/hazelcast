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

import com.hazelcast.sql.impl.calcite.validate.functions.DistributedAvgAggFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlBinaryOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlCaseOperator;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlCastFunction;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastSqlMonotonicBinaryOperator;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastInferTypes;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastReturnTypes;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAllNull;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAny;

/**
 * Custom functions and operators.
 */
public final class HazelcastSqlOperatorTable extends ReflectiveSqlOperatorTable {

    //@formatter:off

    public static final SqlFunction CAST = new HazelcastSqlCastFunction();

    public static final SqlOperator CASE = new HazelcastSqlCaseOperator();

    //#region Predicates.

    public static final SqlBinaryOperator AND = new HazelcastSqlBinaryOperator(
        "AND",
        SqlKind.AND,
        24,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.BOOLEAN,
        notAny(OperandTypes.BOOLEAN_BOOLEAN)
    );

    public static final SqlBinaryOperator OR = new HazelcastSqlBinaryOperator(
        "OR",
        SqlKind.OR,
        22,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.BOOLEAN,
        notAny(OperandTypes.BOOLEAN_BOOLEAN)
    );

    public static final SqlPrefixOperator NOT = new SqlPrefixOperator(
        "NOT",
        SqlKind.NOT,
        26,
        ReturnTypes.ARG0,
        InferTypes.BOOLEAN,
        notAny(OperandTypes.BOOLEAN)
    );

    //#endregion

    //#region Comparison operators.

    public static final SqlBinaryOperator EQUALS = new SqlBinaryOperator(
        "=",
        SqlKind.EQUALS,
        30,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        HazelcastInferTypes.FIRST_KNOWN,
        notAny(OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED)
    );

    public static final SqlBinaryOperator NOT_EQUALS = new SqlBinaryOperator(
        "<>",
        SqlKind.NOT_EQUALS,
        30,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        HazelcastInferTypes.FIRST_KNOWN,
        notAny(OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED)
    );

    public static final SqlBinaryOperator GREATER_THAN = new SqlBinaryOperator(
        ">",
        SqlKind.GREATER_THAN,
        30,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        HazelcastInferTypes.FIRST_KNOWN,
        notAny(HazelcastOperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED)
    );

    public static final SqlBinaryOperator GREATER_THAN_OR_EQUAL = new SqlBinaryOperator(
        ">=",
        SqlKind.GREATER_THAN_OR_EQUAL,
        30,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        HazelcastInferTypes.FIRST_KNOWN,
        notAny(HazelcastOperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED)
    );

    public static final SqlBinaryOperator LESS_THAN = new SqlBinaryOperator(
        "<",
        SqlKind.LESS_THAN,
        30,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        HazelcastInferTypes.FIRST_KNOWN,
        notAny(HazelcastOperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED)
    );

    public static final SqlBinaryOperator LESS_THAN_OR_EQUAL = new SqlBinaryOperator(
        "<=",
        SqlKind.LESS_THAN_OR_EQUAL,
        30,
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
        40,
        true,
        HazelcastReturnTypes.PLUS,
        HazelcastInferTypes.FIRST_KNOWN,
        notAllNull(notAny(OperandTypes.PLUS_OPERATOR))
    );

    public static final SqlBinaryOperator MINUS = new HazelcastSqlMonotonicBinaryOperator(
        "-",
        SqlKind.MINUS,
        40,
        true,
        HazelcastReturnTypes.MINUS,
        HazelcastInferTypes.FIRST_KNOWN,
        notAllNull(notAny(OperandTypes.MINUS_OPERATOR))
    );

    public static final SqlBinaryOperator MULTIPLY = new HazelcastSqlMonotonicBinaryOperator(
        "*",
        SqlKind.TIMES,
        60,
        true,
        HazelcastReturnTypes.MULTIPLY,
        HazelcastInferTypes.FIRST_KNOWN,
        notAllNull(notAny(OperandTypes.MULTIPLY_OPERATOR))
    );

    public static final SqlBinaryOperator DIVIDE = new HazelcastSqlBinaryOperator(
        "/",
        SqlKind.DIVIDE,
        60,
        true,
        HazelcastReturnTypes.DIVIDE,
        HazelcastInferTypes.FIRST_KNOWN,
        notAllNull(notAny(OperandTypes.DIVISION_OPERATOR))
    );

    public static final SqlPrefixOperator UNARY_PLUS = new SqlPrefixOperator(
        "+",
        SqlKind.PLUS_PREFIX,
        80,
        ReturnTypes.ARG0,
        InferTypes.RETURN_TYPE,
        notAllNull(notAny(OperandTypes.NUMERIC_OR_INTERVAL))
    );

    public static final SqlPrefixOperator UNARY_MINUS = new SqlPrefixOperator(
        "-",
        SqlKind.MINUS_PREFIX,
        80,
        HazelcastReturnTypes.UNARY_MINUS,
        InferTypes.RETURN_TYPE,
        notAllNull(notAny(OperandTypes.NUMERIC_OR_INTERVAL))
    );

    //#endregion

    //#region Other custom functions and operators.

    public static final SqlFunction LENGTH = new SqlFunction(
        "LENGTH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        null,
        notAny(OperandTypes.CHARACTER),
        SqlFunctionCategory.NUMERIC
    );

    /** Function to calculate distributed average. */
    public static final SqlAggFunction DISTRIBUTED_AVG = new DistributedAvgAggFunction();

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
