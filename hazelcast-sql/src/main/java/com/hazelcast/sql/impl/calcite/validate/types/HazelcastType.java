/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.validate.types;

import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Custom Hazelcast type that maps to a well-known type name.
 */
public class HazelcastType extends BasicSqlType {
    HazelcastType(SqlTypeName typeName, boolean nullable) {
        this(typeName, nullable, PRECISION_NOT_SPECIFIED);
    }

    HazelcastType(SqlTypeName typeName, boolean nullable, int precision) {
        super(HazelcastTypeSystem.INSTANCE, typeName, precision);

        this.isNullable = nullable;

        computeDigest();
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(HazelcastTypeUtils.toHazelcastType(typeName).getTypeFamily().getPublicType());
    }
}
