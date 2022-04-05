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

import java.time.OffsetDateTime;

/**
 * Converter for {@link OffsetDateTime} type.
 */
public final class OffsetDateTimeConverter extends AbstractTimestampWithTimezoneConverter {

    public static final OffsetDateTimeConverter INSTANCE = new OffsetDateTimeConverter();

    private OffsetDateTimeConverter() {
        super(ID_OFFSET_DATE_TIME);
    }

    @Override
    public Class<?> getValueClass() {
        return OffsetDateTime.class;
    }

    @Override
    public OffsetDateTime asTimestampWithTimezone(Object val) {
        return (OffsetDateTime) val;
    }
}
