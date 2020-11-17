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

package com.hazelcast.sql.impl.calcite.validate.operators;

import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingManualOverride;
import com.hazelcast.sql.impl.calcite.validate.binding.SqlCallBindingOverride;
import com.hazelcast.sql.impl.calcite.validate.operand.DoubleOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.operand.VarcharOperandChecker;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastInferTypes;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public final class HazelcastSqlStringFunction extends SqlFunction implements SqlCallBindingManualOverride {
    private HazelcastSqlStringFunction(String name, SqlReturnTypeInference returnTypeInference) {
        super(
            name,
            SqlKind.OTHER_FUNCTION,
            returnTypeInference,
            HazelcastInferTypes.explicitSingle(VARCHAR),
            null,
            SqlFunctionCategory.STRING
        );
    }

    public static HazelcastSqlStringFunction withStringReturn(String name) {
        // TODO: Return VARCHAR_NULLABLE?
        return new HazelcastSqlStringFunction(name, ReturnTypes.ARG0_NULLABLE);
    }

    public static HazelcastSqlStringFunction withIntegerReturn(String name) {
        return new HazelcastSqlStringFunction(name, ReturnTypes.INTEGER_NULLABLE);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        return VarcharOperandChecker.INSTANCE.check(new SqlCallBindingOverride(callBinding), throwOnFailure, 0);
    }
}
