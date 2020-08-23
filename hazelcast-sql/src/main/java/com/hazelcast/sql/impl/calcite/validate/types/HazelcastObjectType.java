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

import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Represents Hazelcast OBJECT type for Calcite type system.
 * <p>
 * Basically the same as {@link BasicSqlType} of {@link SqlTypeName#ANY} type,
 * but has a more friendly name "OBJECT" instead of "ANY".
 */
public final class HazelcastObjectType extends BasicSqlType {

    /**
     * Non-nullable instance of Hazelcast OBJECT type.
     */
    public static final RelDataType INSTANCE = new HazelcastObjectType(false);

    /**
     * Nullable instance of Hazelcast OBJECT type.
     */
    public static final RelDataType NULLABLE_INSTANCE = new HazelcastObjectType(true);

    private HazelcastObjectType(boolean nullable) {
        super(HazelcastTypeSystem.INSTANCE, SqlTypeName.ANY);
        this.isNullable = nullable;

        // recompute the digest to reflect the nullability of the type
        computeDigest();
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(QueryDataTypeFamily.OBJECT.name());
    }
}
