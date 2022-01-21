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

package com.hazelcast.jet.sql.impl.schema;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import static com.hazelcast.jet.sql.impl.validate.operators.typeinference.HazelcastReturnTypeInference.wrap;

public abstract class HazelcastTableFunction extends SqlFunction implements SqlTableFunction {

    private final SqlReturnTypeInference returnTypeInference;

    protected HazelcastTableFunction(
            String name,
            SqlOperandMetadata operandMetadata,
            SqlReturnTypeInference returnTypeInference
    ) {
        super(
                name,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.CURSOR,
                operandMetadata.typeInference(),
                operandMetadata,
                SqlFunctionCategory.USER_DEFINED_TABLE_SPECIFIC_FUNCTION
        );

        this.returnTypeInference = wrap(returnTypeInference);
    }

    @Override
    public final SqlReturnTypeInference getRowTypeInference() {
        return returnTypeInference;
    }
}
