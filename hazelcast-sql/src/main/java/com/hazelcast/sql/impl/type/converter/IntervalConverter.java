/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import com.hazelcast.sql.impl.type.SqlYearMonthInterval;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTERVAL_DAY_SECOND;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTERVAL_YEAR_MONTH;

@SerializableByConvention
public class IntervalConverter extends Converter {

    public static final IntervalConverter YEAR_MONTH = new IntervalConverter(
        ID_INTERVAL_YEAR_MONTH,
        INTERVAL_YEAR_MONTH,
        SqlYearMonthInterval.class
    );

    public static final IntervalConverter DAY_SECOND = new IntervalConverter(
        ID_INTERVAL_DAY_SECOND,
        INTERVAL_DAY_SECOND,
        SqlDaySecondInterval.class
    );

    private final Class<?> valueClass;

    public IntervalConverter(int id, QueryDataTypeFamily typeFamily, Class<?> valueClass) {
        super(id, typeFamily);

        this.valueClass = valueClass;
    }

    @Override
    public Class<?> getValueClass() {
        return valueClass;
    }

    @Override
    public Object convertToSelf(Converter converter, Object val) {
        Object val0 = converter.asObject(val);

        if (val0 == null) {
            return null;
        }

        QueryDataTypeFamily family = getTypeFamily();

        if (family == INTERVAL_YEAR_MONTH) {
            if (val0 instanceof SqlYearMonthInterval) {
                return val0;
            }
        } else {
            assert family == INTERVAL_DAY_SECOND;

            if (val0 instanceof SqlDaySecondInterval) {
                return val0;
            }
        }

        throw converter.cannotConvertError(family);
    }

    @Override
    public String toString() {
        return "IntervalConverter{" + getTypeFamily() + '}';
    }
}
