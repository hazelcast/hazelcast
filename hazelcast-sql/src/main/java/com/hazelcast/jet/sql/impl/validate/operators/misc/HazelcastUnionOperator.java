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

package com.hazelcast.jet.sql.impl.validate.operators.misc;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastSetOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.Objects;

public class HazelcastUnionOperator extends HazelcastSetOperator {
    public static final HazelcastUnionOperator UNION = new HazelcastUnionOperator("UNION", false);
    public static final HazelcastUnionOperator UNION_ALL = new HazelcastUnionOperator("UNION ALL", true);

    protected HazelcastUnionOperator(String name, boolean all) {
        super(name,
                SqlKind.UNION,
                SqlStdOperatorTable.UNION.getLeftPrec(),
                all
        );
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        return Objects.requireNonNull(getOperandTypeChecker()).checkOperandTypes(callBinding, throwOnFailure);
    }
}
