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

package com.hazelcast.jet.sql.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalTableInsert;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalTableSink;
import com.hazelcast.jet.sql.impl.parse.SqlExtendedInsert;
import com.hazelcast.jet.sql.impl.validate.HazelcastResources;
import com.hazelcast.jet.sql.impl.validate.literal.Literal;
import com.hazelcast.jet.sql.impl.validate.literal.LiteralUtils;
import com.hazelcast.jet.sql.impl.validate.operators.json.HazelcastJsonParseFunction;
import com.hazelcast.jet.sql.impl.validate.operators.json.HazelcastJsonValueFunction;
import com.hazelcast.jet.sql.impl.validate.operators.predicate.HazelcastBetweenOperator;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.HazelcastReturnTypeInference;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJsonEmptyOrError;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlJsonValueReturning;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.Util;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.isNullOrUnknown;
import static java.util.Arrays.asList;
import static org.apache.calcite.avatica.util.TimeUnit.DAY;
import static org.apache.calcite.avatica.util.TimeUnit.MONTH;
import static org.apache.calcite.avatica.util.TimeUnit.SECOND;
import static org.apache.calcite.avatica.util.TimeUnit.YEAR;

/**
 * Custom Hazelcast sql-to-rel converter.
 * <p>
 * Currently, this custom sql-to-rel converter is used to workaround quirks of
 * the default Calcite sql-to-rel converter and to facilitate generation of
 * literals and casts with more precise types assigned during the validation.
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity"})
public final class HazelcastSqlToRelConverter extends SqlToRelConverter {

    private static final SqlIntervalQualifier INTERVAL_YEAR_MONTH = new SqlIntervalQualifier(YEAR, MONTH, SqlParserPos.ZERO);
    private static final SqlIntervalQualifier INTERVAL_DAY_SECOND = new SqlIntervalQualifier(DAY, SECOND, SqlParserPos.ZERO);

    /**
     * See {@link #convertCall(SqlNode, Blackboard)} for more information.
     */
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
            return convertLiteral((SqlLiteral) node, blackboard.getTypeFactory());
        } else if (node.getKind() == SqlKind.CAST) {
            return convertCast((SqlCall) node, blackboard);
        } else if (node.getKind() == SqlKind.IN || node.getKind() == SqlKind.NOT_IN) {
            return convertIn((SqlCall) node, blackboard);
        } else if (node.getKind() == SqlKind.BETWEEN) {
            return convertBetween((SqlCall) node, blackboard);
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
    private RexNode convertLiteral(SqlLiteral literal, RelDataTypeFactory typeFactory) {
        RelDataType type = validator.getValidatedNodeType(literal);

        Object value;

        if (HazelcastTypeUtils.isIntervalType(type) && !SqlUtil.isNullLiteral(literal, false)) {
            // Normalize interval literals to YEAR-MONTH or DAY-SECOND literals.
            value = literal.getValueAs(BigDecimal.class);

            SqlTypeFamily family = type.getSqlTypeName().getFamily();

            if (family == SqlTypeFamily.INTERVAL_YEAR_MONTH) {
                type = typeFactory.createSqlIntervalType(INTERVAL_YEAR_MONTH);
            } else {
                assert family == SqlTypeFamily.INTERVAL_DAY_TIME;

                type = typeFactory.createSqlIntervalType(INTERVAL_DAY_SECOND);
            }
        } else {
            value = literal.getValue();
        }

        return getRexBuilder().makeLiteral(value, type, true);
    }

    /**
     * Convert CAST expression fixing several Apache Calcite problems with literals along the way (see inline JavaDoc).
     */
    private RexNode convertCast(SqlCall call, Blackboard blackboard) {
        SqlNode operand = call.operand(0);
        RexNode convertedOperand = blackboard.convertExpression(operand);

        RelDataType from = validator.getValidatedNodeType(operand);
        RelDataType to = validator.getValidatedNodeType(call);

        QueryDataType fromType = HazelcastTypeUtils.toHazelcastType(from);
        QueryDataType toType = HazelcastTypeUtils.toHazelcastType(to);

        Literal literal = LiteralUtils.literal(convertedOperand);

        if (literal != null && !isNullOrUnknown(((RexLiteral) convertedOperand).getTypeName())) {
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
                QueryDataType actualFromType = HazelcastTypeUtils.toHazelcastTypeFromSqlTypeName(literal.getTypeName());
                toType.getConverter().convertToSelf(actualFromType.getConverter(), literal.getValue());
            } catch (Exception e) {
                throw literalConversionException(validator, call, literal, toType, e);
            }

            // Normalize BOOLEAN and DOUBLE literals when converting them to VARCHAR.
            // BOOLEAN literals are converted to "true"/"false" instead of "TRUE"/"FALSE".
            // DOUBLE literals are converted to a string with scientific conventions (e.g., 1.1E1 instead of 11.0);
            if (SqlTypeName.CHAR_TYPES.contains(to.getSqlTypeName())) {
                return getRexBuilder().makeLiteral(literal.getStringValue(), to, true);
            }

            // There is a bug in RexSimplify that adds an unnecessary second. For example, the string literal "00:00" is
            // converted to 00:00:01. The problematic code is located in DateTimeUtils.timeStringToUnixDate.
            // To workaround the problem, we perform the conversion manually.
            if (SqlTypeName.CHAR_TYPES.contains(from.getSqlTypeName()) && to.getSqlTypeName() == SqlTypeName.TIME) {
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

        if (literal != null && HazelcastTypeUtils.isJsonType(to)) {
            return getRexBuilder().makeCall(HazelcastJsonParseFunction.INSTANCE, convertedOperand);
        }

        // Delegate to Apache Calcite.
        return getRexBuilder().makeCast(to, convertedOperand);
    }

    /**
     * This method overrides Apache Calcite's approach for IN operator.
     *
     * @see org.apache.calcite.sql2rel.SqlToRelConverter##substituteSubQuery
     * @see org.apache.calcite.sql2rel.SqlToRelConverter##convertInToOr
     */
    private RexNode convertIn(SqlCall call, Blackboard blackboard) {
        assert call.getOperandList().size() == 2;
        final SqlNode lhs = call.operand(0);

        final List<RexNode> leftKeys;
        if (lhs.getKind() == SqlKind.ROW) {
            leftKeys = new ArrayList<>();
            for (SqlNode sqlExpr : ((SqlBasicCall) lhs).getOperandList()) {
                leftKeys.add(blackboard.convertExpression(sqlExpr));
            }
        } else {
            leftKeys = ImmutableList.of(blackboard.convertExpression(lhs));
        }

        final SqlNode rhs = call.operand(1);
        if (rhs instanceof SqlNodeList) {
            SqlNodeList valueList = (SqlNodeList) rhs;
            return convertInToOr(
                    blackboard,
                    leftKeys,
                    valueList,
                    (SqlInOperator) call.getOperator()
            );
        }
        throw QueryException.error(SqlErrorCode.GENERIC,
                "Sub-queries are not supported for IN operator.");
    }

    /**
     * Convert "val BETWEEN lower_bound AND upper_bound" expression to
     * 1. If ASYMMETRIC : "val >= lower_bound AND val =< upper_bound" expression. Default mode.
     * 2. If SYMMETRIC : "(val >= lower_bound AND val =< upper_bound) OR (val <= lower_bound AND val >= upper_bound)"
     * expression
     */
    public RexNode convertBetween(SqlCall call, Blackboard blackboard) {
        SqlOperator currentOperator = call.getOperator();
        assert currentOperator instanceof HazelcastBetweenOperator;

        final RexBuilder rexBuilder = getRexBuilder();
        final HazelcastBetweenOperator betweenOp = (HazelcastBetweenOperator) currentOperator;
        final List<RexNode> list = convertExpressionList(
                rexBuilder, blackboard, call.getOperandList(), betweenOp.getOperandTypeChecker().getConsistency()
        );
        final RexNode valueOperand = list.get(SqlBetweenOperator.VALUE_OPERAND);
        final RexNode lowerOperand = list.get(SqlBetweenOperator.LOWER_OPERAND);
        final RexNode upperOperand = list.get(SqlBetweenOperator.UPPER_OPERAND);

        RexNode ge1 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, valueOperand, lowerOperand);
        RexNode le1 = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, valueOperand, upperOperand);
        RexNode and1 = rexBuilder.makeCall(SqlStdOperatorTable.AND, ge1, le1);

        RexNode res;
        final SqlBetweenOperator.Flag symmetric = betweenOp.getFlag();
        switch (symmetric) {
            case ASYMMETRIC:
                res = and1;
                break;
            case SYMMETRIC:
                RexNode ge2 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, valueOperand, upperOperand);
                RexNode le2 = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, valueOperand, lowerOperand);
                RexNode and2 = rexBuilder.makeCall(SqlStdOperatorTable.AND, ge2, le2);
                res = rexBuilder.makeCall(SqlStdOperatorTable.OR, and1, and2);
                break;
            default:
                throw Util.unexpected(symmetric);
        }
        if (betweenOp.isNegated()) {
            res = rexBuilder.makeCall(SqlStdOperatorTable.NOT, res);
        }
        return res;
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

        if (((SqlCall) node).getOperator() instanceof HazelcastJsonValueFunction) {
            return convertJsonValueCall((SqlCall) node, blackboard);
        }

        if (callSet.add(node)) {
            try {
                RelDataType type = validator.getValidatedNodeType(node);

                HazelcastReturnTypeInference.push(type);

                try {
                    return blackboard.convertExpression(node);
                } catch (RuntimeException e) {
                    // For some operators Calcite does reflective call to validate the AST
                    if (e.getCause() instanceof InvocationTargetException && e.getCause().getCause() instanceof QueryException) {
                        throw (QueryException) e.getCause().getCause();
                    } else {
                        throw e;
                    }
                } finally {
                    HazelcastReturnTypeInference.pop();
                }
            } finally {
                callSet.remove(node);
            }
        }

        return null;
    }

    /**
     * Converts JSON_VALUE calls with extended syntax, with RETURNING clause among other things.
     * Because there is no RexNode for type reference in Calcite (see CAST implementation),
     * the type has to be instead set as the type of the parent (JSON_VALUE's RexCall), which is
     * then interpreted as the desired type of the expression.
     * <p>
     * Supported syntax:
     * JSON_VALUE(jsonArg, jsonPathArg [returning] [onEmpty|onError])
     * returning: RETURNING dataType
     * onEmpty: (DEFAULT value | NULL | ERROR) ON EMPTY
     * onError: (DEFAULT value | NULL | ERROR) ON ERROR
     */
    private RexNode convertJsonValueCall(SqlCall call, Blackboard bb) {
        RexNode target = bb.convertExpression(call.operand(0));
        RexNode path = bb.convertExpression(call.operand(1));

        SqlJsonValueEmptyOrErrorBehavior onError = SqlJsonValueEmptyOrErrorBehavior.NULL;
        SqlJsonValueEmptyOrErrorBehavior onEmpty = SqlJsonValueEmptyOrErrorBehavior.NULL;

        RelDataType returning = validator.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);

        RexNode defaultValueOnError = getRexBuilder().makeNullLiteral(typeFactory.createSqlType(SqlTypeName.ANY));
        RexNode defaultValueOnEmpty = getRexBuilder().makeNullLiteral(typeFactory.createSqlType(SqlTypeName.ANY));

        // Start at 3rd Arg
        int tokenIndex = 2;

        // RETURNING can only be placed at the beginning, never in the middle or the end of the list of tokens.
        if (call.operandCount() > 2 && isJsonValueReturningClause(call.operand(tokenIndex))) {
            returning = validator.getValidatedNodeType(call.operand(tokenIndex + 1));
            tokenIndex += 2;
        }

        boolean onEmptyDefined = false;
        boolean onErrorDefined = false;

        while (tokenIndex < call.operandCount()) {
            if (!(call.operand(tokenIndex) instanceof SqlLiteral)) {
                throw QueryException.error(SqlErrorCode.PARSING, "Unsupported JSON_VALUE extended syntax");
            }

            final SqlJsonValueEmptyOrErrorBehavior behavior = (SqlJsonValueEmptyOrErrorBehavior)
                    ((SqlLiteral) call.operand(tokenIndex)).getValue();

            RexNode defaultExpr = getRexBuilder().makeNullLiteral(typeFactory.createSqlType(SqlTypeName.ANY));

            if (behavior == null) {
                throw QueryException.error(SqlErrorCode.PARSING,
                        "Failed to extract ON behavior for JSON_VALUE call");
            }

            switch (behavior) {
                case DEFAULT:
                    defaultExpr = bb.convertExpression(call.operand(tokenIndex + 1));
                    tokenIndex += 2;
                    break;
                case NULL:
                case ERROR:
                    tokenIndex++;
                    break;
                default:
                    // guard against possible unsupported updates to syntax, should never be thrown.
                    throw QueryException.error(SqlErrorCode.PARSING,
                            "Unsupported JSON_VALUE OnEmptyOrErrorBehavior");
            }

            final SqlJsonEmptyOrError onTarget = (SqlJsonEmptyOrError) ((SqlLiteral) call.operand(tokenIndex))
                    .getValue();
            if (onTarget == null) {
                throw QueryException.error(SqlErrorCode.PARSING,
                        "Failed to extract ON-behavior target for JSON_VALUE call");
            }

            switch (onTarget) {
                case EMPTY:
                    if (onEmptyDefined) {
                        throw QueryException.error(SqlErrorCode.PARSING,
                                "Duplicate ON EMPTY clause in JSON_VALUE call");
                    }
                    if (behavior == SqlJsonValueEmptyOrErrorBehavior.DEFAULT) {
                        defaultValueOnEmpty = defaultExpr;
                    }
                    onEmpty = behavior;
                    onEmptyDefined = true;
                    break;
                case ERROR:
                    if (onErrorDefined) {
                        throw QueryException.error(SqlErrorCode.PARSING,
                                "Duplicate ON ERROR clause in JSON_VALUE call");
                    }
                    if (behavior == SqlJsonValueEmptyOrErrorBehavior.DEFAULT) {
                        defaultValueOnError = defaultExpr;
                    }
                    onError = behavior;
                    onErrorDefined = true;
                    break;
                default:
                    // guard against possible unsupported updates to syntax, should never be thrown.
                    throw QueryException.error(SqlErrorCode.PARSING,
                            "Unsupported JSON_VALUE EmptyOrErrorBehavior target");
            }
            tokenIndex++;
        }

        return getRexBuilder().makeCall(returning, HazelcastJsonValueFunction.INSTANCE, asList(
                target,
                path,
                defaultValueOnEmpty,
                defaultValueOnError,
                bb.convertLiteral(onEmpty.symbol(SqlParserPos.ZERO)),
                bb.convertLiteral(onError.symbol(SqlParserPos.ZERO))
        ));
    }

    private boolean isJsonValueReturningClause(SqlNode node) {
        return node instanceof SqlLiteral && ((SqlLiteral) node).getValue() instanceof SqlJsonValueReturning;
    }

    private static List<RexNode> convertExpressionList(RexBuilder rexBuilder,
                                                       Blackboard bb,
                                                       List<SqlNode> nodes,
                                                       SqlOperandTypeChecker.Consistency consistency
    ) {
        if (nodes.size() == 1) {
            return Collections.singletonList(bb.convertExpression(nodes.get(0)));
        }

        final List<RexNode> exprs = new ArrayList<>();

        for (SqlNode node : nodes) {
            exprs.add(bb.convertExpression(node));
        }
        if (exprs.size() > 1) {
            final RelDataType type = consistentType(bb, consistency, RexUtil.types(exprs));
            if (type != null) {
                final List<RexNode> oldExpressions = Lists.newArrayList(exprs);
                exprs.clear();
                for (RexNode expr : oldExpressions) {
                    exprs.add(rexBuilder.ensureType(type, expr, true));
                }
            }
        }
        return exprs;
    }

    private static RelDataType consistentType(Blackboard bb,
                                              SqlOperandTypeChecker.Consistency consistency,
                                              List<RelDataType> types
    ) {
        switch (consistency) {
            case COMPARE:
                final List<RelDataType> nonCharacterTypes = types.stream()
                        .filter(type -> type.getFamily() != SqlTypeFamily.CHARACTER)
                        .collect(Collectors.toList());

                if (!nonCharacterTypes.isEmpty()) {
                    types = enlargeNumericTypes(bb, types.size(), nonCharacterTypes);
                }
                // fall through
            case LEAST_RESTRICTIVE:
                return bb.getTypeFactory().leastRestrictive(types);
            default:
                return null;
        }
    }

    private static List<RelDataType> enlargeNumericTypes(Blackboard bb,
                                                         final int typeCount,
                                                         List<RelDataType> nonCharacterTypes) {
        if (nonCharacterTypes.size() < typeCount) {
            final RelDataTypeFamily family = nonCharacterTypes.get(0).getFamily();
            if (family instanceof SqlTypeFamily) {
                // The character arguments might be larger than the numeric argument
                switch ((SqlTypeFamily) family) {
                    case INTEGER:
                    case NUMERIC:
                        nonCharacterTypes.add(bb.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
                        break;
                    default:
                        break;
                }
            }
        }
        return nonCharacterTypes;
    }

    private static QueryException literalConversionException(
            SqlValidator validator,
            SqlCall call,
            Literal literal,
            QueryDataType toType,
            Exception e
    ) {
        String literalValue = literal.getStringValue();

        if (SqlTypeName.CHAR_TYPES.contains(literal.getTypeName())) {
            literalValue = "'" + literalValue + "'";
        }

        Resources.ExInst<SqlValidatorException> contextError = HazelcastResources.RESOURCES.cannotCastLiteralValue(
                literalValue,
                toType.getTypeFamily().getPublicType().toString(),
                e.getMessage()
        );

        CalciteContextException calciteContextError = validator.newValidationError(call, contextError);

        throw QueryException.error(SqlErrorCode.PARSING, calciteContextError.getMessage(), e);
    }

    // Copied from SqlToRelConverter.
    private RexNode convertInToOr(
            final Blackboard bb,
            final List<RexNode> leftKeys,
            SqlNodeList valuesList,
            SqlInOperator op
    ) {
        final List<RexNode> comparisons = constructComparisons(bb, leftKeys, valuesList);

        switch (op.kind) {
            case ALL:
                return RexUtil.composeConjunction(rexBuilder, comparisons, true);
            case NOT_IN:
                return rexBuilder.makeCall(SqlStdOperatorTable.NOT,
                        RexUtil.composeDisjunction(rexBuilder, comparisons, true));
            case IN:
            case SOME:
                return RexUtil.composeDisjunction(rexBuilder, comparisons, true);
            default:
                throw new AssertionError();
        }
    }

    /**
     * Constructs comparisons between
     * left-hand operand (as a rule, SqlIdentifier) and right-hand list.
     */
    private List<RexNode> constructComparisons(
            Blackboard bb,
            List<RexNode> leftKeys,
            SqlNodeList valuesList
    ) {
        final List<RexNode> comparisons = new ArrayList<>();

        for (SqlNode rightValues : valuesList) {
            RexNode rexComparison;
            final SqlOperator comparisonOp = SqlStdOperatorTable.EQUALS;
            if (leftKeys.size() == 1) {
                rexComparison = rexBuilder.makeCall(
                        comparisonOp,
                        leftKeys.get(0),
                        ensureSqlType(
                                leftKeys.get(0).getType(),
                                bb.convertExpression(rightValues)
                        )
                );
            } else {
                assert rightValues instanceof SqlCall;
                final SqlBasicCall basicCall = (SqlBasicCall) rightValues;
                assert basicCall.getOperator() instanceof SqlRowOperator && basicCall.operandCount() == leftKeys.size();
                rexComparison = RexUtil.composeConjunction(rexBuilder,
                        Pair.zip(leftKeys, basicCall.getOperandList()).stream().map(pair ->
                                rexBuilder.makeCall(
                                        comparisonOp, pair.left, ensureSqlType(
                                                pair.left.getType(),
                                                bb.convertExpression(pair.right)
                                        )
                                )
                        ).collect(Collectors.toList()));
            }
            comparisons.add(rexComparison);
        }
        return comparisons;
    }

    private RexNode ensureSqlType(RelDataType type, RexNode node) {
        if (type.getSqlTypeName() == node.getType().getSqlTypeName()
                || (type.getSqlTypeName() == SqlTypeName.VARCHAR
                && node.getType().getSqlTypeName() == SqlTypeName.CHAR)) {
            return node;
        }
        return rexBuilder.ensureType(type, node, true);
    }

    @Override
    protected RelNode convertInsert(SqlInsert insert0) {
        SqlExtendedInsert insert = (SqlExtendedInsert) insert0;
        TableModify modify = (TableModify) super.convertInsert(insert);
        return insert.isInsert() ? new LogicalTableInsert(modify) : new LogicalTableSink(modify);
    }
}
