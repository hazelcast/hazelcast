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

import com.hazelcast.sql.impl.calcite.validate.types.HazelcastInferTypes;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.notAny;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastOperandTypes.wrap;

/**
 * Function that accepts a DOUBLE argument and produces a DOUBLE result.
 */
public class HazelcastDoubleFunction extends SqlFunction {
    public HazelcastDoubleFunction(String name) {
        super(
            name,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.DOUBLE_NULLABLE,
            HazelcastInferTypes.explicit(SqlTypeName.DOUBLE),
            wrap(notAny(OperandTypes.NUMERIC)),
            SqlFunctionCategory.NUMERIC
        );
    }
}
