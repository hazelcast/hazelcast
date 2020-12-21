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

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.calcite.validate.HazelcastResources;
import com.hazelcast.sql.impl.calcite.validate.literal.Literal;
import com.hazelcast.sql.impl.calcite.validate.literal.LiteralUtils;
import com.hazelcast.sql.impl.calcite.validate.operators.HazelcastReturnTypeInference;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.BigDecimalConverter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.TimeString;

import java.math.BigDecimal;
import java.time.LocalTime;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;

/**
 * Custom Hazelcast sql-to-rel converter.
 * <p>
 * Currently, this custom sql-to-rel converter is used to workaround quirks of
 * the default Calcite sql-to-rel converter and to facilitate generation of
 * literals and casts with more precise types assigned during the validation.
 */
public class HazelcastSqlToRelConverter extends SqlToRelConverter {
    /** See {@link #convertCall(SqlNode, Blackboard)} for more information. */
    private final Set<SqlNode> callSet = Collections.newSetFromMap(new IdentityHashMap<>());

    public HazelcastSqlToRelConverter(
        RelOptTable.ViewExpander viewExpander,
        SqlValidator validator,
        Prepare.CatalogReader catalogReader,
        RelOptCluster cluster,
        SqlRexConvertletTable convertletTable,
        Config config
    ) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
    }

    @Override
    protected RexNode convertExtendedExpression(SqlNode node, Blackboard blackboard) {
        // Hook into conversion of literals, casts and calls to execute our own logic.
        if (node.getKind() == SqlKind.LITERAL) {
            return convertLiteral((SqlLiteral) node);
        } else if (node.getKind() == SqlKind.CAST) {
            return convertCast((SqlCall) node, blackboard);
        } else if (node instanceof SqlCall) {
            return convertCall(node, blackboard);
        }

        return null;
    }

    /**
     * Convert the literal taking in count the type that we assigned to it during validation.
     * Otherwise Apache Calcite will try to deduce literal type again, leading to incorrect exposed types.
     * <p>
     * For example, {@code [x:BIGINT > 1]} is interpreted as {@code [x:BIGINT > 1:BIGINT]} during the validation.
     * If this method is not invoked, Apache Calcite will convert it to {[@code x:BIGINT > 1:TINYINT]} instead.
     */
    private RexNode convertLiteral(SqlLiteral literal) {
        RelDataType type = validator.getValidatedNodeType(literal);

        return getRexBuilder().makeLiteral(literal.getValue(), type, true);
    }

    /**
     * Convert CAST exception fixing several Apache Calcite problems with literals along the way (see inline JavaDoc).
     */
    private RexNode convertCast(SqlCall call, Blackboard blackboard) {
        SqlNode operand = call.operand(0);
        RexNode convertedOperand = blackboard.convertExpression(operand);

        RelDataType from = validator.getValidatedNodeType(operand);
        RelDataType to = validator.getValidatedNodeType(call);

        QueryDataType fromType = HazelcastTypeUtils.toHazelcastType(from.getSqlTypeName());
        QueryDataType toType = HazelcastTypeUtils.toHazelcastType(to.getSqlTypeName());

        Literal literal = LiteralUtils.literal(operand);

        if (literal != null && literal.getTypeName() != NULL) {
            // There is a bug in RexSimplify that incorrectly converts numeric literals from one numeric type to another.
            // The problem is located in the RexToLixTranslator.translateLiteral. To perform a conversion, it delegates
            // to Primitive.number(Number) method, that does a conversion without checking for overflow. For example, the
            // expression [32767 AS TINYINT] is converted to -1, which is obviously incorrect.
            // To workaround the problem, we perform the conversion using our converters manually. If the conversion fails,
            // we throw an error (it would have been thrown in runtime anyway), thus preventing Apache Calcite from entering
            // the problematic simplification routine.
            // Since this workaround moves conversion errors to the parsing phase, we conduct the conversion check for all
            // types to ensure that we throw consistent error messages for all literal-related conversions errors.
            try {
                toType.getConverter().convertToSelf(fromType.getConverter(), literal.getValue());
            } catch (Exception e) {
                throw literalConversionException(validator, call, literal, toType, e);
            }

            // Normalize BOOLEAN and DOUBLE literals when converting them to VARCHAR.
            // BOOLEAN literals are converted to "true"/"false" instead of "TRUE"/"FALSE".
            // DOUBLE literals are converted to a string with scientific conventions (e.g., 1.1E1 instead of 11.0);
            if (CHAR_TYPES.contains(to.getSqlTypeName())) {
                return getRexBuilder().makeLiteral(literal.getStringValue(), to, true);
            }

            // There is a bug in RexSimplify that adds an unnecessary second. For example, the string literal "00:00" is
            // converted to 00:00:01. The problematic code is located in DateTimeUtils.timeStringToUnixDate.
            // To workaround the problem, we perform the conversion manually.
            if (CHAR_TYPES.contains(from.getSqlTypeName()) && to.getSqlTypeName() == TIME) {
                LocalTime time = fromType.getConverter().asTime(literal.getStringValue());

                TimeString timeString = new TimeString(time.getHour(), time.getMinute(), time.getSecond());

                return getRexBuilder().makeLiteral(timeString, to, true);
            }

            // Apache Calcite uses an expression simplification logic that treats CASTs with inexacat literals incorrectly.
            // For example, "CAST(1.0 as DOUBLE) = CAST(1.0000000000000001 as DOUBLE)" is converted to "false", while it should
            // be "true". See CastFunctionIntegrationTest.testApproximateTypeSimplification - it will fail without this fix.
            if (fromType.getTypeFamily().isNumeric()) {
                if (toType.getTypeFamily().isNumericApproximate()) {
                    BigDecimal originalValue = ((SqlLiteral) operand).getValueAs(BigDecimal.class);
                    Object convertedValue = toType.getConverter().convertToSelf(BigDecimalConverter.INSTANCE, originalValue);

                    return getRexBuilder().makeLiteral(convertedValue, to, false);
                }
            }
        }

        // Delegate to Apache Calcite.
        return getRexBuilder().makeCast(to, convertedOperand);
    }

    /**
     * This method overcomes a bug in Apache Calcite that ignores previously resolved return types of the expression
     * and instead attempts to infer them again using different logic. Without this fix, we will get type resolution
     * errors after SQL-to-rel conversion.
     * <p>
     * The method relies on the fact that all operators use {@link HazelcastReturnTypeInference} as a top-level return type
     * inference method.
     * <ul>
     *     <li>When a call node is observed for the first time, get it's return type and save it to a thread local variable</li>
     *     <li>Then delegate back to original converter code</li>
     *     <li>When converter attempts to resolve the return type of a call, he will get the previously saved type from
     *     the thread-local variable</li>
     * </ul>
     */
    private RexNode convertCall(SqlNode node, Blackboard blackboard) {
        if (callSet.add(node)) {
            try {
                RelDataType type = validator.getValidatedNodeType(node);

                HazelcastReturnTypeInference.push(type);

                try {
                    return blackboard.convertExpression(node);
                } finally {
                    HazelcastReturnTypeInference.pop();
                }
            } finally {
                callSet.remove(node);
            }
        }

        return null;
    }

    private static QueryException literalConversionException(
        SqlValidator validator,
        SqlCall call,
        Literal literal,
        QueryDataType toType,
        Exception e
    ) {
        String literalValue = literal.getStringValue();

        if (CHAR_TYPES.contains(literal.getTypeName())) {
            literalValue = "'" + literalValue + "'";
        }

        Resources.ExInst<SqlValidatorException> contextError = HazelcastResources.RESOURCES.cannotCastLiteralValue(
            literalValue,
            toType.getTypeFamily().getPublicType().name(),
            e.getMessage()
        );

        CalciteContextException calciteContextError = validator.newValidationError(call, contextError);

        throw QueryException.error(SqlErrorCode.PARSING, calciteContextError.getMessage(), calciteContextError);
    }
}
