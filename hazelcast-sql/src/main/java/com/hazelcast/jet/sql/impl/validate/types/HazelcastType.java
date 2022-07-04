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

package com.hazelcast.jet.sql.impl.validate.types;

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
        sb.append(HazelcastTypeUtils.toHazelcastType(this).getTypeFamily().getPublicType());
    }
}
