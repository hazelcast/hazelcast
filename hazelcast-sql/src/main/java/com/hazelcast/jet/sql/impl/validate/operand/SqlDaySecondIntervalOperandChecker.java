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

package com.hazelcast.jet.sql.impl.validate.operand;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.ParameterConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeFamily;

public final class SqlDaySecondIntervalOperandChecker extends AbstractOperandChecker {

    public static final SqlDaySecondIntervalOperandChecker INSTANCE = new SqlDaySecondIntervalOperandChecker();

    private SqlDaySecondIntervalOperandChecker() {
    }

    @Override
    protected boolean matchesTargetType(RelDataType operandType) {
        return operandType.getSqlTypeName().getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME;
    }

    @Override
    protected boolean coerce(
            HazelcastSqlValidator validator,
            HazelcastCallBinding callBinding,
            SqlNode operand,
            RelDataType operandType,
            int operandIndex
    ) {
        return false;
    }

    @Override
    protected RelDataType getTargetType(RelDataTypeFactory factory, boolean nullable) {
        // interval dynamic params not supported yet
        throw new UnsupportedOperationException();
    }

    @Override
    protected ParameterConverter parameterConverter(SqlDynamicParam operand) {
        // interval dynamic params not supported yet
        throw new UnsupportedOperationException();
    }
}
