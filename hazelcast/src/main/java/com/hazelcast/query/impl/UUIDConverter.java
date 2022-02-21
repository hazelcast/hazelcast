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

package com.hazelcast.query.impl;

import java.util.UUID;

/**
 * TypeConverter to handle UUID
 */
final class UUIDConverter extends TypeConverters.BaseTypeConverter {

    @Override
    Comparable convertInternal(Comparable value) {
        if (value instanceof UUID) {
            return value;
        }
        if (value instanceof String) {
            return UUID.fromString((String) value);
        }
        throw new IllegalArgumentException("Cannot convert [" + value + "] to java.util.UUID");
    }
}
