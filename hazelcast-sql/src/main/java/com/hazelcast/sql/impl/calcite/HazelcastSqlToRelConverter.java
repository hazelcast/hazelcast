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

import com.hazelcast.sql.impl.calcite.parse.SqlCreateExternalTable;
import com.hazelcast.sql.impl.calcite.parse.SqlTableColumn;
import com.hazelcast.sql.impl.calcite.schema.HazelcastRelOptTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.schema.UnknownStatistic;
import com.hazelcast.sql.impl.schema.ExternalCatalog;
import com.hazelcast.sql.impl.schema.ExternalTable;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.BigDecimalConverter;
import com.hazelcast.sql.impl.type.converter.BooleanConverter;
import com.hazelcast.sql.impl.type.converter.DoubleConverter;
import com.hazelcast.sql.impl.type.converter.StringConverter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.isChar;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.isNumeric;
import static java.util.Arrays.asList;
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
 * literals and casts with a more precise types assigned during the validation.
 */
public final class HazelcastSqlToRelConverter extends SqlToRelConverter {

    private final ExternalCatalog catalog;

    public HazelcastSqlToRelConverter(
        RelOptTable.ViewExpander viewExpander,
        SqlValidator validator,
        Prepare.CatalogReader catalogReader,
        RelOptCluster cluster,
        SqlRexConvertletTable convertletTable,
        Config config,
        ExternalCatalog catalog
    ) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);

        this.catalog = catalog;
    }

    @Override
    public RelRoot convertQuery(
        SqlNode query,
        boolean needsValidation,
        boolean top
    ) {
        if (query instanceof SqlCreateExternalTable) {
            return convertCreateTable((SqlCreateExternalTable) query);
        } else {
            return super.convertQuery(query, needsValidation, top);
        }
    }

    private RelRoot convertCreateTable(SqlCreateExternalTable create) {
        assert create.source() != null : "source cannot be null";

        RelNode convertedSource = super.convertQuery(create.source(), false, true).rel;
        RelDataType rowType = convertedSource.getRowType();

        List<ExternalField> externalFields = new ArrayList<>();
        Iterator<SqlTableColumn> columns = create.columns().iterator();
        for (RelDataTypeField relField : rowType.getFieldList()) {
            SqlTableColumn column = columns.hasNext() ? columns.next() : null;

            String name = column != null ? column.name() : relField.getName();
            QueryDataType type = SqlToQueryType.map(relField.getType().getSqlTypeName());
            String externalName = column != null ? column.externalName() : null;

            externalFields.add(new ExternalField(name, type, externalName));
        }
        ExternalTable externalTable = new ExternalTable(create.name(), create.type(), externalFields, create.options());

        Table table = catalog.toTable(externalTable);
        RelOptTableImpl relTable = RelOptTableImpl.create(
                catalogReader,
                rowType,
                asList(table.getSchemaName(), table.getName()),
                new HazelcastTable(table, UnknownStatistic.INSTANCE),
                null
        );
        RelOptTable hazelcastRelTable = new HazelcastRelOptTable(relTable);

        LogicalTableModify logicalTableModify = LogicalTableModify.create(
                hazelcastRelTable,
                catalogReader,
                convertedSource,
                Operation.INSERT,
                null,
                null,
                false
        );

        return RelRoot.of(logicalTableModify, rowType, SqlKind.INSERT);
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

    private RexNode convertCast(SqlCall call, Blackboard blackboard) {
        RelDataType to = validator.getValidatedNodeType(call);
        RexNode operand = blackboard.convertExpression(call.operand(0));

        if (SqlUtil.isNullLiteral(call.operand(0), false)) {
            // Just generate the cast without messing with the value: it's
            // always NULL.
            return getRexBuilder().makeCast(to, operand);
        }

        RelDataType from = operand.getType();

        // Use our to-string conversions for REAL, DOUBLE and BOOLEAN;
        // Calcite does conversions using its own formatting.
        if (operand.isA(SqlKind.LITERAL) && isChar(to)) {
            RexLiteral literal = (RexLiteral) operand;

            switch (from.getSqlTypeName()) {
                case REAL:
                case DOUBLE:
                    BigDecimal decimalValue = literal.getValueAs(BigDecimal.class);
                    String decimalAsString = DoubleConverter.INSTANCE.asVarchar(decimalValue.doubleValue());
                    return getRexBuilder().makeLiteral(decimalAsString, to, true);
                case BOOLEAN:
                    boolean booleanValue = literal.getValueAs(Boolean.class);
                    String booleanAsString = BooleanConverter.INSTANCE.asVarchar(booleanValue);
                    return getRexBuilder().makeLiteral(booleanAsString, to, true);
                default:
                    // do nothing
            }
        }

        // Convert REAL/DOUBLE values from BigDecimal representation to
        // REAL/DOUBLE and back, otherwise Calcite might think two floating point
        // values having the same REAL/DOUBLE representation are distinct since
        // their BigDecimal representations might differ.
        if (operand.isA(SqlKind.LITERAL) && isNumeric(from) && APPROX_TYPES.contains(to.getSqlTypeName())) {
            RexLiteral literal = (RexLiteral) operand;
            BigDecimal value = literal.getValueAs(BigDecimal.class);

            if (to.getSqlTypeName() == DOUBLE) {
                value = BigDecimal.valueOf(BigDecimalConverter.INSTANCE.asDouble(value));
            } else {
                assert to.getSqlTypeName() == REAL;
                value = new BigDecimal(Float.toString(BigDecimalConverter.INSTANCE.asReal(value)));
            }

            return getRexBuilder().makeLiteral(value, to, false);
        }

        // also removes the cast if it's not required
        return getRexBuilder().makeCast(to, operand);
    }

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
        // REAL/DOUBLE and back, otherwise Calcite might think two floating point
        // values having the same REAL/DOUBLE representation are distinct since
        // their BigDecimal representations might differ.
        if (type.getSqlTypeName() == DOUBLE) {
            value = BigDecimal.valueOf(BigDecimalConverter.INSTANCE.asDouble(value));
        } else if (type.getSqlTypeName() == REAL) {
            value = new BigDecimal(Float.toString(BigDecimalConverter.INSTANCE.asReal(value)));
        }

        // Generate the literal.

        // Internally, all string literals in Calcite have CHAR type, but we
        // interpret all strings as having VARCHAR type. By allowing the casting
        // for VARCHAR, we emit string literals of VARCHAR type. The cast itself
        // is optimized away, so we left with just a literal.
        boolean allowCast = type.getSqlTypeName() == VARCHAR;
        return getRexBuilder().makeLiteral(value, type, allowCast);
    }

}
