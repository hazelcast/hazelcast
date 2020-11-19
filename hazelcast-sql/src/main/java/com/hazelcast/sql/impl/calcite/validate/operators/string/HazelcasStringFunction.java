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

package com.hazelcast.sql.impl.calcite.validate.operators.string;

import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingManualOverride;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.operand.VarcharOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.types.ReplaceUnknownOperandTypeInference;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public final class HazelcasStringFunction extends SqlFunction implements SqlCallBindingManualOverride {

    public static final SqlFunction ASCII = HazelcasStringFunction.withIntegerReturn("ASCII");
    public static final SqlFunction INITCAP = HazelcasStringFunction.withStringReturn("INITCAP");

    public static final SqlFunction CHAR_LENGTH = HazelcasStringFunction.withIntegerReturn("CHAR_LENGTH");
    public static final SqlFunction CHARACTER_LENGTH = HazelcasStringFunction.withIntegerReturn("CHARACTER_LENGTH");
    public static final SqlFunction LENGTH = HazelcasStringFunction.withIntegerReturn("LENGTH");

    public static final SqlFunction LOWER = HazelcasStringFunction.withStringReturn("LOWER");
    public static final SqlFunction UPPER = HazelcasStringFunction.withStringReturn("UPPER");

    public static final SqlFunction RTRIM = HazelcasStringFunction.withStringReturn("RTRIM");
    public static final SqlFunction LTRIM = HazelcasStringFunction.withStringReturn("LTRIM");
    public static final SqlFunction BTRIM = HazelcasStringFunction.withStringReturn("BTRIM");

    private HazelcasStringFunction(String name, SqlReturnTypeInference returnTypeInference) {
        super(
            name,
            SqlKind.OTHER_FUNCTION,
            returnTypeInference,
            new ReplaceUnknownOperandTypeInference(VARCHAR),
            null,
            SqlFunctionCategory.STRING
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        return VarcharOperandChecker.INSTANCE.check(new SqlCallBindingOverride(callBinding), throwOnFailure, 0);
    }

    private static HazelcasStringFunction withStringReturn(String name) {
        return new HazelcasStringFunction(name, ReturnTypes.ARG0_NULLABLE);
    }

    private static HazelcasStringFunction withIntegerReturn(String name) {
        return new HazelcasStringFunction(name, ReturnTypes.INTEGER_NULLABLE);
    }
}
