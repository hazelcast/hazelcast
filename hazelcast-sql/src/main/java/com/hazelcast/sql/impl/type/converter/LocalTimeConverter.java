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

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Converter for {@link java.time.LocalTime} type.
 */
@SerializableByConvention
public final class LocalTimeConverter extends AbstractTemporalConverter {

    public static final LocalTimeConverter INSTANCE = new LocalTimeConverter();

    private LocalTimeConverter() {
        super(ID_LOCAL_TIME, QueryDataTypeFamily.TIME);
    }

    @Override
    public Class<?> getValueClass() {
        return LocalTime.class;
    }

    @Override
    public String asVarchar(Object val) {
        return cast(val).toString();
    }

    @Override
    public LocalTime asTime(Object val) {
        return cast(val);
    }

    @Override
    public LocalDateTime asTimestamp(Object val) {
        LocalTime time = cast(val);

        return timeToTimestamp(time);
    }

    @Override
    public OffsetDateTime asTimestampWithTimezone(Object val) {
        LocalTime time = cast(val);

        LocalDateTime timestamp = timeToTimestamp(time);

        return timestampToTimestampWithTimezone(timestamp);
    }

    @Override
    public Object convertToSelf(Converter valConverter, Object val) {
        return valConverter.asTime(val);
    }

    private LocalTime cast(Object val) {
        return ((LocalTime) val);
    }
}
