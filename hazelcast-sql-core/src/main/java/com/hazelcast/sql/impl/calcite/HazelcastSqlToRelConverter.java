/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
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
     * Convert a literal taking into account the type that we assigned to it during validation.
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
     * Convert CAST expression fixing several Apache Calcite problems with literals along the way (see inline JavaDoc).
     */
    private RexNode convertCast(SqlCall call, Blackboard blackboard) {
        SqlNode operand = call.operand(0);
        RexNode convertedOperand = blackboard.convertExpression(operand);

        RelDataType from = validator.getValidatedNodeType(operand);
        RelDataType to = validator.getValidatedNodeType(call);

        QueryDataType fromType = HazelcastTypeUtils.toHazelcastType(from.getSqlTypeName());
        QueryDataType toType = HazelcastTypeUtils.toHazelcastType(to.getSqlTypeName());

        Literal literal = LiteralUtils.literal(convertedOperand);

        if (literal != null && ((RexLiteral) convertedOperand).getTypeName() != NULL) {
            // There is a bug in RexBuilder.makeCast(). If the operand is a literal, it can directly return a literal with the
            // desired target type instead of an actual cast, but when doing that it doesn't check for numeric overflow.
            // For example if this method is converting [128 AS TINYINT] is converted to -1, which is obviously incorrect.
            // It should have failed.
            // To workaround the problem, we perform the conversion using our converters manually. If the conversion fails,
            // we throw an error (it would have been thrown if the conversion was performed at runtime anyway), before
            // delegating to RexBuilder.makeCast().
            // Since this workaround moves conversion errors to the parsing phase, we conduct the conversion check for all
            // types to ensure that we throw consistent error messages for all literal-related conversions errors.
            try {
                // The literal's type might be different from the operand type for example here:
                //     CAST(CAST(42 AS SMALLINT) AS TINYINT)
                // The operand of the outer cast is validated as a SMALLINT, however the operand, thanks to the
                // simplification in RexBuilder.makeCast(), is converted to a literal [42:SMALLINT]. And LiteralUtils converts
                // this operand to [42:TINYINT] - we have to use the literal's type instead of the validated operand type.
                QueryDataType actualFromType = HazelcastTypeUtils.toHazelcastType(literal.getTypeName());
                toType.getConverter().convertToSelf(actualFromType.getConverter(), literal.getValue());
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

            // Apache Calcite uses an expression simplification logic that treats CASTs with inexact literals incorrectly.
            // For example, "CAST(1.0 as DOUBLE) = CAST(1.0000000000000001 as DOUBLE)" is converted to "false", while it should
            // be "true". See CastFunctionIntegrationTest.testApproximateTypeSimplification - it will fail without this fix.
            if (fromType.getTypeFamily().isNumeric()) {
                if (toType.getTypeFamily().isNumericApproximate()) {
                    Converter converter = Converters.getConverter(literal.getValue().getClass());
                    Object convertedValue = toType.getConverter().convertToSelf(converter, literal.getValue());

                    return getRexBuilder().makeLiteral(convertedValue, to, false);
                }
            }
        }

        // Delegate to Apache Calcite.
        return getRexBuilder().makeCast(to, convertedOperand);
    }

    /**
     * This method overcomes a bug in Apache Calcite that ignores previously resolved return types of the expression
     * and instead attempts to infer them again using a different logic. Without this fix, we will get type resolution
     * errors after a SQL-to-rel conversion.
     * <p>
     * The method relies on the fact that all operators use {@link HazelcastReturnTypeInference} as a top-level return type
     * inference method.
     * <ul>
     *     <li>When a call node is observed for the first time, get its return type and save it to a thread-local variable
     *     <li>Then delegate back to original converter code
     *     <li>When converter attempts to resolve the return type of a call, it will get the previously saved type from
     *     the thread-local variable
     * </ul>
     */
    private RexNode convertCall(SqlNode node, Blackboard blackboard) {
        // Ignore DEFAULT (used for default function arguments). This node isn't
        // present in the original parse tree and at the validation time, but is
        // added later when converting by SqlCallBinding.permutedCall(), which
        // adjusts the actual function arguments to the formal arguments, adding
        // this node for the missing arguments. If we called getValidatedNodeType()
        // for DEFAULT node, it will fail.
        if (node.getKind() == SqlKind.DEFAULT) {
            return null;
        }

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

        throw QueryException.error(SqlErrorCode.PARSING, calciteContextError.getMessage(), e);
    }
}
