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

package com.hazelcast.jet.sql.impl.validate.operators.datetime;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operand.AnyOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operand.OperandCheckerProgram;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public final class HazelcastToCharFunction extends HazelcastFunction {
    public static final HazelcastToCharFunction INSTANCE = new HazelcastToCharFunction();

    private HazelcastToCharFunction() {
        super(
                "TO_CHAR",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.explicit(VARCHAR),
                new ReplaceUnknownOperandTypeInference(new SqlTypeName[]{ANY, VARCHAR, VARCHAR}),
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(2, 3);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding binding, boolean throwOnFailure) {
        switch (binding.getOperandCount()) {
            case 2:
                return new OperandCheckerProgram(
                        AnyOperandChecker.INSTANCE,
                        TypedOperandChecker.VARCHAR
                ).check(binding, throwOnFailure);
            case 3:
                return new OperandCheckerProgram(
                        AnyOperandChecker.INSTANCE,
                        TypedOperandChecker.VARCHAR,
                        TypedOperandChecker.VARCHAR
                ).check(binding, throwOnFailure);
            default:
                throw new AssertionError();
        }
    }
}
