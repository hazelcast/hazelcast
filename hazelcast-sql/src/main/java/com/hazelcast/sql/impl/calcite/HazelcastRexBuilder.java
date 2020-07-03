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

import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.REAL;

/**
 * Custom Hazelcast expression builder.
 * <p>
 * Currently, this custom expression builder is used just to workaround quirks
 * of the default Calcite expression builder.
 */
public final class HazelcastRexBuilder extends RexBuilder {

    public HazelcastRexBuilder(RelDataTypeFactory typeFactory) {
        super(typeFactory);
        assert typeFactory instanceof HazelcastTypeFactory;
    }

    @Override
    public RexNode makeLiteral(Object value, RelDataType type, boolean allowCast) {
        // XXX: Calcite evaluates casts like CAST(0 AS ANY) statically and
        // assigns imprecise types: BIGINT for any integer value and DOUBLE for
        // any floating-point value (except BigDecimal). The code bellow fixes
        // that.

        if (type.getSqlTypeName() == ANY && value instanceof Number) {
            if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
                Number number = (Number) value;
                int bitWidth = HazelcastIntegerType.bitWidthOf(number.longValue());
                type = HazelcastIntegerType.of(bitWidth, false);
            } else if (value instanceof Float) {
                type = HazelcastTypeFactory.INSTANCE.createSqlType(REAL);
            }
        }

        return super.makeLiteral(value, type, allowCast);
    }

}
