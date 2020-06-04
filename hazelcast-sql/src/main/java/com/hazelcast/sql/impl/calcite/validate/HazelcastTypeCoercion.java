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

package com.hazelcast.sql.impl.calcite.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;

public final class HazelcastTypeCoercion extends TypeCoercionImpl {

    public HazelcastTypeCoercion(RelDataTypeFactory typeFactory, HazelcastSqlValidator validator) {
        super(typeFactory, validator);
    }

    @Override
    protected boolean booleanEquality(SqlCallBinding binding, RelDataType left, RelDataType right) {
        // disallow coercion from BOOLEAN to numeric types
        return false;
    }

}
