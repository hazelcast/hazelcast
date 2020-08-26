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

package com.hazelcast.sql.impl.calcite.validate.types;

import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Class describing TIME, TIMESTAMP and TIMESTAMP_WITH_TIME_ZONE types.
 */
public final class HazelcastTemporalType extends BasicSqlType {

    public static final RelDataType TIME = new HazelcastTemporalType(SqlTypeName.TIME, false);
    public static final RelDataType TIME_NULLABLE = new HazelcastTemporalType(SqlTypeName.TIME, true);

    public static final RelDataType TIMESTAMP = new HazelcastTemporalType(SqlTypeName.TIMESTAMP, false);
    public static final RelDataType TIMESTAMP_NULLABLE = new HazelcastTemporalType(SqlTypeName.TIMESTAMP, true);

    public static final RelDataType TIMESTAMP_WITH_TIME_ZONE = new HazelcastTemporalType(
        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, false
    );

    public static final RelDataType TIMESTAMP_WITH_TIME_ZONE_NULLABLE = new HazelcastTemporalType(
        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        true
    );

    private HazelcastTemporalType(SqlTypeName typeName, boolean nullable) {
        super(HazelcastTypeSystem.INSTANCE, typeName);

        this.isNullable = nullable;

        computeDigest();
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(SqlToQueryType.map(typeName).getTypeFamily());
    }
}
