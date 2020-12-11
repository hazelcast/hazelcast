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

import com.hazelcast.sql.impl.type.converter.BigDecimalConverter;
import com.hazelcast.sql.impl.type.converter.BooleanConverter;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.StringConverter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import java.math.BigDecimal;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.isChar;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.isNumeric;
import static com.hazelcast.sql.impl.expression.math.ExpressionMath.DECIMAL_MATH_CONTEXT;
import static org.apache.calcite.sql.type.SqlTypeName.APPROX_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.REAL;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

/**
 * Custom Hazelcast sql-to-rel converter.
 * <p>
 * Currently, this custom sql-to-rel converter is used to workaround quirks of
 * the default Calcite sql-to-rel converter and to facilitate generation of
 * literals and casts with more precise types assigned during the validation.
 */
public class HazelcastSqlToRelConverter extends SqlToRelConverter {

    public HazelcastSqlToRelConverter(RelOptTable.ViewExpander viewExpander, SqlValidator validator,
                                      Prepare.CatalogReader catalogReader, RelOptCluster cluster,
                                      SqlRexConvertletTable convertletTable, Config config) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
    }

    @Override
    protected RexNode convertExtendedExpression(SqlNode node, Blackboard blackboard) {
        if (node.getKind() == SqlKind.LITERAL) {
            return convertLiteral((SqlLiteral) node);
        } else if (node.getKind() == SqlKind.CAST) {
            return convertCast((SqlCall) node, blackboard);
        }

        return null;
    }

    @SuppressWarnings("UnpredictableBigDecimalConstructorCall")
    private RexNode convertCast(SqlCall call, Blackboard blackboard) {
        RelDataType to = validator.getValidatedNodeType(call);
        RexNode operand = blackboard.convertExpression(call.operand(0));

        if (SqlUtil.isNullLiteral(call.operand(0), false)) {
            // Just generate the cast without messing with the value: it's
            // always NULL.
            return getRexBuilder().makeCast(to, operand);
        }

        RelDataType from = operand.getType();

        // Use our to-string conversions for floating point types and BOOLEAN,
        // Calcite does conversions using its own formatting.
        if (operand.isA(SqlKind.LITERAL) && isChar(to)) {
            RexLiteral literal = (RexLiteral) operand;

            switch (from.getSqlTypeName()) {
                case REAL:
                case DOUBLE:
                case DECIMAL:
                    BigDecimal decimalValue = literal.getValueAs(BigDecimal.class);
                    Converter fromConverter = SqlToQueryType.map(from.getSqlTypeName()).getConverter();
                    Object value = fromConverter.convertToSelf(BigDecimalConverter.INSTANCE, decimalValue);
                    Object valueAsString = StringConverter.INSTANCE.convertToSelf(fromConverter, value);

                    return getRexBuilder().makeLiteral(valueAsString, to, true);
                case BOOLEAN:
                    boolean booleanValue = literal.getValueAs(Boolean.class);
                    String booleanAsString = BooleanConverter.INSTANCE.asVarchar(booleanValue);
                    return getRexBuilder().makeLiteral(booleanAsString, to, true);
                default:
                    // do nothing
            }
        }

        // Convert REAL/DOUBLE values from BigDecimal representation to
        // REAL/DOUBLE and back, otherwise Calcite might think two floating-point
        // values having the same REAL/DOUBLE representation are distinct since
        // their BigDecimal representations might differ.
        if (operand.isA(SqlKind.LITERAL) && isNumeric(from) && APPROX_TYPES.contains(to.getSqlTypeName())) {
            RexLiteral literal = (RexLiteral) operand;
            BigDecimal value = literal.getValueAs(BigDecimal.class);

            if (to.getSqlTypeName() == DOUBLE) {
                value = new BigDecimal(BigDecimalConverter.INSTANCE.asDouble(value), DECIMAL_MATH_CONTEXT);
            } else {
                assert to.getSqlTypeName() == REAL;
                value = new BigDecimal(BigDecimalConverter.INSTANCE.asReal(value), DECIMAL_MATH_CONTEXT);
            }

            return getRexBuilder().makeLiteral(value, to, false);
        }

        // also removes the cast if it's not required
        return getRexBuilder().makeCast(to, operand);
    }

    @SuppressWarnings("UnpredictableBigDecimalConstructorCall")
    private RexNode convertLiteral(SqlLiteral literal) {
        if (literal.getValue() == null) {
            // trust Calcite on generation for NULL literals
            return null;
        }

        RelDataType type = validator.getValidatedNodeType(literal);
        SqlTypeName literalTypeName = literal.getTypeName();

        // Extract the literal value.

        Object value;
        if (CHAR_TYPES.contains(literalTypeName)) {
            if (isNumeric(type)) {
                value = StringConverter.INSTANCE.asDecimal(literal.getValueAs(String.class));
            } else if (BOOLEAN_TYPES.contains(type.getSqlTypeName())) {
                value = StringConverter.INSTANCE.asBoolean(literal.getValueAs(String.class));
            } else {
                value = literal.getValue();
            }
        } else if (INTERVAL_TYPES.contains(literalTypeName)) {
            value = literal.getValueAs(BigDecimal.class);
        } else {
            value = literal.getValue();
        }

        // Convert REAL/DOUBLE values from BigDecimal representation to
        // REAL/DOUBLE and back, otherwise Calcite might think two floating-point
        // values having the same REAL/DOUBLE representation are distinct since
        // their BigDecimal representations might differ.
        if (type.getSqlTypeName() == DOUBLE) {
            value = new BigDecimal(BigDecimalConverter.INSTANCE.asDouble(value), DECIMAL_MATH_CONTEXT);
        } else if (type.getSqlTypeName() == REAL) {
            value = new BigDecimal(BigDecimalConverter.INSTANCE.asReal(value), DECIMAL_MATH_CONTEXT);
        }

        // Generate the literal.

        // Internally, all string literals in Calcite have CHAR type, but we
        // interpret all strings as having VARCHAR type. By allowing the casting
        // for VARCHAR, we emit string literals of VARCHAR type. The cast itself
        // is optimized away, so we're left with just a literal.
        boolean allowCast = type.getSqlTypeName() == VARCHAR;
        return getRexBuilder().makeLiteral(value, type, allowCast);
    }

}
