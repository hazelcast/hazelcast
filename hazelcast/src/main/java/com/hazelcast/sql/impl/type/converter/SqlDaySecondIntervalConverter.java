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

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;

/**
 * Converter for {@link SqlDaySecondInterval} type.
 */
public final class SqlDaySecondIntervalConverter extends Converter {
    /** Singleton instance. */
    public static final SqlDaySecondIntervalConverter INSTANCE = new SqlDaySecondIntervalConverter();

    private SqlDaySecondIntervalConverter() {
        super(ID_INTERVAL_DAY_SECOND, QueryDataTypeFamily.INTERVAL_DAY_SECOND);
    }

    @Override
    public Class<?> getValueClass() {
        return SqlDaySecondInterval.class;
    }

    @NotConvertible
    public Object asObject(Object val) {
        throw cannotConvert(QueryDataTypeFamily.OBJECT, val);
    }

    @Override
    public Object convertToSelf(Converter converter, Object val) {
        if (val instanceof SqlDaySecondInterval) {
            return val;
        }

        throw cannotConvert(converter.getTypeFamily(), getTypeFamily(), val);
    }
}
