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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlMonotonicity;

import static org.apache.calcite.sql.type.SqlTypeName.NULL;

public class HazelcastSqlBinaryOperator extends SqlBinaryOperator {

    public HazelcastSqlBinaryOperator(String name, SqlKind kind, int prec, boolean leftAssoc,
                                      SqlReturnTypeInference returnTypeInference, SqlOperandTypeInference operandTypeInference,
                                      SqlOperandTypeChecker operandTypeChecker) {
        super(name, kind, prec, leftAssoc, returnTypeInference, operandTypeInference, operandTypeChecker);
    }

    @Override
    public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        RelDataType left = call.getOperandType(0);
        RelDataType right = call.getOperandType(1);

        if (left.getSqlTypeName() == NULL || right.getSqlTypeName() == NULL) {
            return SqlMonotonicity.CONSTANT;
        }

        return super.getMonotonicity(call);
    }

}
