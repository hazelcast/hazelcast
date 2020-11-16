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

import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastInferTypes;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastReturnTypes;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlMonotonicity;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAllNull;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAny;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.wrap;

/**
 * The same as {@link SqlBinaryOperator}, but supports monotonicity for NULL
 * literals and operators from our custom {@link HazelcastSqlOperatorTable}.
 */
public class HazelcastDivideOperator extends SqlBinaryOperator {
    public HazelcastDivideOperator() {
        super(
            "/",
            SqlKind.DIVIDE,
            SqlStdOperatorTable.DIVIDE.getLeftPrec(),
            true,
            HazelcastReturnTypes.DIVIDE,
            HazelcastInferTypes.FIRST_KNOWN,
            wrap(notAllNull(notAny(OperandTypes.DIVISION_OPERATOR)))
        );
    }

    @Override
    public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        // The original method does the same, but doesn't consider NULLs to be producing results with the constant monotonicity.
        if (call.isOperandNull(0, true) || call.isOperandNull(1, true)) {
            return SqlMonotonicity.CONSTANT;
        }

        return super.getMonotonicity(call);
    }
}
