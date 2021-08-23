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

package com.hazelcast.jet.sql.impl.validate.operators.string;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operand.TypedOperandChecker;
import com.hazelcast.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public final class HazelcastStringFunction extends HazelcastFunction {

    public static final SqlFunction ASCII = HazelcastStringFunction.withIntegerReturn("ASCII");
    public static final SqlFunction INITCAP = HazelcastStringFunction.withStringReturn("INITCAP");

    public static final SqlFunction CHAR_LENGTH = HazelcastStringFunction.withIntegerReturn("CHAR_LENGTH");
    public static final SqlFunction CHARACTER_LENGTH = HazelcastStringFunction.withIntegerReturn("CHARACTER_LENGTH");
    public static final SqlFunction LENGTH = HazelcastStringFunction.withIntegerReturn("LENGTH");

    public static final SqlFunction LOWER = HazelcastStringFunction.withStringReturn("LOWER");
    public static final SqlFunction UPPER = HazelcastStringFunction.withStringReturn("UPPER");

    public static final SqlFunction RTRIM = HazelcastStringFunction.withStringReturn("RTRIM");
    public static final SqlFunction LTRIM = HazelcastStringFunction.withStringReturn("LTRIM");
    public static final SqlFunction BTRIM = HazelcastStringFunction.withStringReturn("BTRIM");

    private HazelcastStringFunction(String name, SqlReturnTypeInference returnTypeInference) {
        super(
                name,
                SqlKind.OTHER_FUNCTION,
                returnTypeInference,
                new ReplaceUnknownOperandTypeInference(VARCHAR),
                SqlFunctionCategory.STRING
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        return TypedOperandChecker.VARCHAR.check(callBinding, throwOnFailure, 0);
    }

    private static HazelcastStringFunction withStringReturn(String name) {
        return new HazelcastStringFunction(name, ReturnTypes.ARG0_NULLABLE);
    }

    private static HazelcastStringFunction withIntegerReturn(String name) {
        return new HazelcastStringFunction(name, ReturnTypes.INTEGER_NULLABLE);
    }
}
