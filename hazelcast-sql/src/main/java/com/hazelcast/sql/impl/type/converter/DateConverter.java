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

import java.time.OffsetDateTime;
import java.util.Date;

/**
 * Converter for {@link java.util.Date} type.
 */
@SerializableByConvention
public final class DateConverter extends AbstractTimestampWithTimezoneConverter {

    public static final DateConverter INSTANCE = new DateConverter();

    private DateConverter() {
        super(ID_DATE);
    }

    @Override
    public Class<?> getValueClass() {
        return Date.class;
    }

    @Override
    public OffsetDateTime asTimestampWithTimezone(Object val) {
        return OffsetDateTime.ofInstant(((Date) val).toInstant(), DEFAULT_ZONE);
    }
}
