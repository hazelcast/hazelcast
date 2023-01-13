/*
 * Copyright 2023 Hazelcast Inc.
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
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastObjectTypeReference;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.AbstractList;
import java.util.Map;
import java.util.Map.Entry;

public class HazelcastToRowFunction extends HazelcastFunction {
    public static final HazelcastFunction INSTANCE = new HazelcastToRowFunction();

    public HazelcastToRowFunction() {
        super(
                "TO_ROW",
                SqlKind.OTHER_FUNCTION,
                null,
                new ReplaceUnknownOperandTypeInference(SqlTypeName.ANY),
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    protected boolean checkOperandTypes(final HazelcastCallBinding callBinding, final boolean throwOnFailure) {
        return callBinding.getOperandType(0) instanceof HazelcastObjectTypeReference;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        // The type of a ROW(e1,e2) expression is a record with the types
        // {e1type,e2type}.  According to the standard, field names are
        // implementation-defined.
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        HazelcastObjectTypeReference operandType = (HazelcastObjectTypeReference) opBinding.getOperandType(0);

        final RelDataType recordType = typeFactory.createStructType(
                new AbstractList<Entry<String, RelDataType>>() {
                    @Override
                    public Map.Entry<String, RelDataType> get(int index) {
                        RelDataTypeField field = operandType.getFieldList().get(index);
                        return Pair.of(field.getName(), field.getType());
                    }

                    @Override
                    public int size() {
                        return operandType.getFieldCount();
                    }
                });

        // The value of ROW(e1,e2) is considered null if and only all of its
        // fields (i.e., e1, e2) are null. Otherwise ROW can not be null.
        final boolean nullable =
                recordType.getFieldList().stream()
                        .allMatch(f -> f.getType().isNullable());
        return typeFactory.createTypeWithNullability(recordType, nullable);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }
}
