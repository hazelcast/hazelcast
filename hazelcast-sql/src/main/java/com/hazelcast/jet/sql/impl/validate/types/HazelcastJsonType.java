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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.type.SqlTypeName;

public final class HazelcastJsonType extends RelDataTypeImpl {
    public static final HazelcastJsonType TYPE_NULLABLE = new HazelcastJsonType(true);
    public static final HazelcastJsonType TYPE = new HazelcastJsonType(false);
    public static final HazelcastJsonType FAMILY = TYPE;

    private final boolean nullable;

    private HazelcastJsonType(boolean nullable) {
        this.nullable = nullable;
        computeDigest();
    }

    public static RelDataType create(boolean nullable) {
        return nullable
                ? TYPE_NULLABLE
                : TYPE;
    }

    @Override
    protected void generateTypeString(final StringBuilder sb, final boolean withDetail) {
        sb.append("JSON");
    }

    @Override
    public SqlTypeName getSqlTypeName() {
        return SqlTypeName.OTHER;
    }


    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public RelDataTypeFamily getFamily() {
        return FAMILY;
    }
}
