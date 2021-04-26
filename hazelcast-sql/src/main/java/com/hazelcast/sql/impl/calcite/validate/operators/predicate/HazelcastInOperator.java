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

package com.hazelcast.sql.impl.calcite.validate.operators.predicate;

import com.google.common.collect.ImmutableList;
import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastOperandTypeCheckerAware;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.CalciteResource;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.ExplicitOperatorBinding;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ComparableOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

public class HazelcastInOperator extends SqlInOperator implements HazelcastOperandTypeCheckerAware {

    public static final HazelcastInOperator IN = new HazelcastInOperator("IN", false);
    public static final HazelcastInOperator NOT_IN = new HazelcastInOperator("NOT IN", true);
    protected static final HazelcastInOperatorResource HZRESOURCE = Resources.create(HazelcastInOperatorResource.class);

    public HazelcastInOperator(String name, boolean negated) {
        super(name, negated ? SqlKind.NOT_IN : SqlKind.IN);
    }

    @Override
    public RelDataType deriveType(
            SqlValidator validator,
            SqlValidatorScope scope,
            SqlCall call) {
        final List<SqlNode> operands = call.getOperandList();
        assert operands.size() == 2;
        final SqlNode left = operands.get(0);
        final SqlNode right = operands.get(1);

        final RelDataTypeFactory typeFactory = validator.getTypeFactory();
        RelDataType leftType = validator.deriveType(scope, left);
        RelDataType rightType;

        // Derive type for RHS.
        if (right instanceof SqlNodeList) {
            // Handle the 'IN (expr, ...)' form.
            List<RelDataType> rightTypeList = new ArrayList<>();
            SqlNodeList nodeList = (SqlNodeList) right;
            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode node = nodeList.get(i);
                RelDataType nodeType = validator.deriveType(scope, node);
                rightTypeList.add(nodeType);
            }
            rightType = typeFactory.leastRestrictive(rightTypeList);

            // First check that the expressions in the IN list are compatible
            // with each other. Same rules as the VALUES operator (per
            // SQL:2003 Part 2 Section 8.4, <in predicate>).
            if (null == rightType && validator.config().typeCoercionEnabled()) {
                // Do implicit type cast if it is allowed to.
                rightType = validator.getTypeCoercion().getWiderTypeFor(rightTypeList, false);
            }
            if (null == rightType) {
                throw validator.newValidationError(right,
                        RESOURCE.incompatibleTypesInList());
            }

            // Record the RHS type for use by SqlToRelConverter.
            ((SqlValidatorImpl) validator).setValidatedNodeType(nodeList, rightType);
        } else {
            // We do not support subquerying for IN operator.
            throw validator.newValidationError(call, HZRESOURCE.noSubQueryAllowed());
        }
        HazelcastCallBinding hazelcastCallBinding = prepareBinding(new SqlCallBinding(validator, scope, call));
        // Coerce type first.
        if (hazelcastCallBinding.isTypeCoercionEnabled()) {
            boolean coerced = hazelcastCallBinding.getValidator().getTypeCoercion()
                    .inOperationCoercion(hazelcastCallBinding);
            if (coerced) {
                // Update the node data type if we coerced any type.
                leftType = validator.deriveType(scope, call.operand(0));
                rightType = validator.deriveType(scope, call.operand(1));
            }
        }

        // Now check that the left expression is compatible with the
        // type of the list. Same strategy as the '=' operator.
        // Normalize the types on both sides to be row types
        // for the purposes of compatibility-checking.
        RelDataType leftRowType = SqlTypeUtil.promoteToRowType(typeFactory, leftType, null);
        RelDataType rightRowType = SqlTypeUtil.promoteToRowType(typeFactory, rightType, null);

        final ComparableOperandTypeChecker checker =
                (ComparableOperandTypeChecker)
                        OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED;
        if (!checker.checkOperandTypes(
                new ExplicitOperatorBinding(
                        hazelcastCallBinding,
                        ImmutableList.of(leftRowType, rightRowType)), hazelcastCallBinding)) {
            throw validator.newValidationError(call,
                    RESOURCE.incompatibleValueType(SqlStdOperatorTable.IN.getName()));
        }

        return typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BOOLEAN),
                anyNullable(leftRowType.getFieldList())
                        || anyNullable(rightRowType.getFieldList())
        );
    }

    private static boolean anyNullable(List<RelDataTypeField> fieldList) {
        for (RelDataTypeField field : fieldList) {
            if (field.getType().isNullable()) {
                return true;
            }
        }
        return false;
    }

    interface HazelcastInOperatorResource extends CalciteResource {
        @Resources.BaseMessage("Sub-queries are not allowed for IN operator.")
        Resources.ExInst<SqlValidatorException> noSubQueryAllowed();
    }
}
