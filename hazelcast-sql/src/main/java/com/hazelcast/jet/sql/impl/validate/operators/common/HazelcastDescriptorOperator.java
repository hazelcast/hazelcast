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

package com.hazelcast.jet.sql.impl.validate.operators.common;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDescriptorOperator;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import static com.hazelcast.jet.sql.impl.validate.operators.typeinference.HazelcastReturnTypeInference.wrap;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Hazelcast equivalent of {@link SqlDescriptorOperator}.
 */
public class HazelcastDescriptorOperator extends SqlOperator implements HazelcastOperandTypeCheckerAware {

    private static final int PRECEDENCE = 100;
    private static final SqlReturnTypeInference RETURN_TYPE_INFERENCE =
            wrap(binding -> binding.getTypeFactory().createSqlType(SqlTypeName.COLUMN_LIST));

    public HazelcastDescriptorOperator() {
        super(
                "DESCRIPTOR",
                SqlKind.DESCRIPTOR,
                PRECEDENCE,
                PRECEDENCE,
                RETURN_TYPE_INFERENCE,
                null,
                null
        );
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.FUNCTION;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.from(1);
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        return validator.getTypeFactory().createSqlType(SqlTypeName.COLUMN_LIST);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding binding, boolean throwOnFailure) {
        HazelcastCallBinding bindingOverride = prepareBinding(binding);
        return checkOperandTypes(bindingOverride, throwOnFailure);
    }

    private boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        for (SqlNode operand : binding.getCall().getOperandList()) {
            if (!(operand instanceof SqlIdentifier) || !((SqlIdentifier) operand).isSimple()) {
                if (throwOnFailure) {
                    throw SqlUtil.newContextException(operand.getParserPosition(), RESOURCE.aliasMustBeSimpleIdentifier());
                } else {
                    return false;
                }
            }
        }
        return true;
    }
}
