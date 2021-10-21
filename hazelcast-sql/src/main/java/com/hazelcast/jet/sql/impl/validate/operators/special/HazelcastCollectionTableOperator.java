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

package com.hazelcast.jet.sql.impl.validate.operators.special;

import com.hazelcast.jet.sql.impl.validate.HazelcastCallBinding;
import com.hazelcast.jet.sql.impl.validate.operators.common.HazelcastSpecialOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCollectionTableOperator;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Hazelcast equivalent of {@link SqlCollectionTableOperator}.
 */
public class HazelcastCollectionTableOperator extends HazelcastSpecialOperator {

    public static final int PRECEDENCE = 200;

    public HazelcastCollectionTableOperator(String name) {
        super(
                name,
                SqlKind.COLLECTION_TABLE,
                PRECEDENCE,
                true,
                ReturnTypes.ARG0,
                null);
    }

    @Override
    protected boolean checkOperandTypes(HazelcastCallBinding callBinding, boolean throwOnFailure) {
        return true;
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlUtil.unparseFunctionSyntax(this, writer, call, false);
    }
}
