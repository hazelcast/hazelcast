/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Converter for {@link java.time.LocalTime} type.
 */
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
