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

package com.hazelcast.jet.sql.impl.validate.operators.misc;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastBinaryOperator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public final class HazelcastWithinGroupOperator extends HazelcastBinaryOperator {

    public static final HazelcastWithinGroupOperator INSTANCE = new HazelcastWithinGroupOperator();

    private static final int PRECEDENCE = 100;

    private HazelcastWithinGroupOperator() {
        super(
                "WITHIN GROUP",
                SqlKind.WITHIN_GROUP,
                PRECEDENCE,
                true,
                ReturnTypes.ARG0,
                null
        );
    }

    @Override
    public boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        return OperandTypes.ANY_IGNORE.checkOperandTypes(callBinding, throwOnFailure);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        return validator.deriveType(scope, call.getOperandList().get(0));
    }
}
