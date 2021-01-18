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

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

public class HazelcastCaseOperator extends SqlOperator {
    protected HazelcastCaseOperator(String name,
                                    SqlKind kind,
                                    int leftPrecedence,
                                    int rightPrecedence,
                                    SqlReturnTypeInference returnTypeInference,
                                    SqlOperandTypeInference operandTypeInference,
                                    SqlOperandTypeChecker operandTypeChecker) {
        super(name, kind, leftPrecedence, rightPrecedence, returnTypeInference, operandTypeInference, operandTypeChecker);
    }

    @Override
    public SqlSyntax getSyntax() {
        return null;
    }
}
