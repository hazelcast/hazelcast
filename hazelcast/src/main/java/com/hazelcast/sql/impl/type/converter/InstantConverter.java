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

import java.time.Instant;
import java.time.OffsetDateTime;

/**
 * Converter for {@link Instant} type.
 */
public final class InstantConverter extends AbstractTimestampWithTimezoneConverter {

    public static final InstantConverter INSTANCE = new InstantConverter();

    private InstantConverter() {
        super(ID_INSTANT);
    }

    @Override
    public Class<?> getValueClass() {
        return Instant.class;
    }

    @Override
    public OffsetDateTime asTimestampWithTimezone(Object val) {
        return OffsetDateTime.ofInstant(((Instant) val), DEFAULT_ZONE);
    }
}
