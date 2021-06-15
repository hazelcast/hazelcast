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

package com.hazelcast.sql.impl.calcite.validate.operators.string;

import com.hazelcast.sql.impl.calcite.validate.HazelcastCallBinding;
import com.hazelcast.sql.impl.calcite.validate.operand.TypedOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operators.ReplaceUnknownOperandTypeInference;
import com.hazelcast.sql.impl.calcite.validate.operators.common.HazelcastFunction;
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
