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

package com.hazelcast.jet.sql.impl.validate.operators.special;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastSpecialOperator;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlMapValueConstructor;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Hazelcast equivalent of {@link SqlMapValueConstructor}.
 */
public class HazelcastMapValueConstructor extends HazelcastSpecialOperator {

    public HazelcastMapValueConstructor() {
        super(
                "MAP",
                SqlKind.MAP_VALUE_CONSTRUCTOR,
                MDX_PRECEDENCE,
                false,
                HazelcastMapValueConstructor::inferReturnType0,
                new ReplaceUnknownOperandTypeInference(SqlTypeName.ANY)
        );
    }

    private static RelDataType inferReturnType0(SqlOperatorBinding binding) {
        Pair<RelDataType, RelDataType> entryType = findEntryType(binding.getTypeFactory(), binding.collectOperandTypes());
        return SqlTypeUtil.createMapType(binding.getTypeFactory(), entryType.left, entryType.right, false);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.any();
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        // do not throwOnFailure as MAP is not really supported - user either will get
        // 'MAP VALUE CONSTRUCTOR not supported' (UnsupportedOperationVisitor) or
        // JetDynamicTableFunction validation will kick in and reject non-VARCHARs
        // obviously fix it when MAP/MAP_VALUE_CONSTRUCTOR gets proper support
        boolean result = checkOperandTypes(callBinding);

        List<RelDataType> argTypes = SqlTypeUtil.deriveAndCollectTypes(
                callBinding.getValidator(),
                callBinding.getScope(),
                callBinding.operands()
        );
        if (argTypes.size() == 0) {
            throw callBinding.newValidationError(RESOURCE.mapRequiresTwoOrMoreArgs());
        }
        if (argTypes.size() % 2 > 0) {
            throw callBinding.newValidationError(RESOURCE.mapRequiresEvenArgCount());
        }
        Pair<RelDataType, RelDataType> entryType = findEntryType(callBinding.getTypeFactory(), argTypes);
        if (entryType.left == null || entryType.right == null) {
            if (throwOnFailure) {
                throw callBinding.newValidationError(RESOURCE.needSameTypeParameter());
            }
            return false;
        }

        return result;
    }

    private static boolean checkOperandTypes(HazelcastCallBinding callBinding) {
        boolean result = true;
        for (int i = 0; i < callBinding.getOperandCount(); i++) {
            // supporting just VARCHARs now
            result &= TypedOperandChecker.VARCHAR.check(callBinding, false, i);
        }
        return result;
    }

    private static Pair<RelDataType, RelDataType> findEntryType(
            RelDataTypeFactory typeFactory,
            List<RelDataType> argTypes
    ) {
        return Pair.of(
                typeFactory.leastRestrictive(Util.quotientList(argTypes, 2, 0)),
                typeFactory.leastRestrictive(Util.quotientList(argTypes, 2, 1))
        );
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        writer.keyword(getName());
        SqlWriter.Frame frame = writer.startList("[", "]");
        for (SqlNode operand : call.getOperandList()) {
            writer.sep(",");
            operand.unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(frame);
    }
}
