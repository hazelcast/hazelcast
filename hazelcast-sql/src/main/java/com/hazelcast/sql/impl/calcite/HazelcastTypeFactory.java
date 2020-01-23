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

package com.hazelcast.sql.impl.calcite;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;

public class HazelcastTypeFactory extends JavaTypeFactoryImpl {
    @Override
    public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
        return super.createTypeWithNullability(type, nullable);
    }

    // TODO: Possible nested fields support. See big TODO in HazelcastTableFields.
//    @Override
//    public RelDataType createSqlType(SqlTypeName typeName) {
//        if (typeName == SqlTypeName.ANY)
//            return new HazelcastTableRelDataType(this, new HazelcastTableFields());
//
//        return super.createSqlType(typeName);
//    }
}
