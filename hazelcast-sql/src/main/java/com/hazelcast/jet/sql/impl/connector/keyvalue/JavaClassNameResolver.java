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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.core.HazelcastJsonValue;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@SuppressWarnings("checkstyle:ExecutableStatementCount")
public final class JavaClassNameResolver {

    private static final Map<String, String> CLASS_NAMES_BY_FORMAT;

    static {
        CLASS_NAMES_BY_FORMAT = new HashMap<>();
        CLASS_NAMES_BY_FORMAT.put("varchar", String.class.getName());
        CLASS_NAMES_BY_FORMAT.put("character varying", String.class.getName());
        CLASS_NAMES_BY_FORMAT.put("char varying", String.class.getName());
        CLASS_NAMES_BY_FORMAT.put("boolean", Boolean.class.getName());
        CLASS_NAMES_BY_FORMAT.put("tinyint", Byte.class.getName());
        CLASS_NAMES_BY_FORMAT.put("smallint", Short.class.getName());
        CLASS_NAMES_BY_FORMAT.put("integer", Integer.class.getName());
        CLASS_NAMES_BY_FORMAT.put("int", Integer.class.getName());
        CLASS_NAMES_BY_FORMAT.put("bigint", Long.class.getName());
        CLASS_NAMES_BY_FORMAT.put("decimal", BigDecimal.class.getName());
        CLASS_NAMES_BY_FORMAT.put("dec", BigDecimal.class.getName());
        CLASS_NAMES_BY_FORMAT.put("numeric", BigDecimal.class.getName());
        CLASS_NAMES_BY_FORMAT.put("real", Float.class.getName());
        CLASS_NAMES_BY_FORMAT.put("double", Double.class.getName());
        CLASS_NAMES_BY_FORMAT.put("double precision", Double.class.getName());
        CLASS_NAMES_BY_FORMAT.put("time", LocalTime.class.getName());
        CLASS_NAMES_BY_FORMAT.put("date", LocalDate.class.getName());
        CLASS_NAMES_BY_FORMAT.put("timestamp", LocalDateTime.class.getName());
        CLASS_NAMES_BY_FORMAT.put("timestamp with time zone", OffsetDateTime.class.getName());
        CLASS_NAMES_BY_FORMAT.put("json", HazelcastJsonValue.class.getName());
    }

    private JavaClassNameResolver() {
    }

    public static String resolveClassName(String format) {
        return CLASS_NAMES_BY_FORMAT.get(format);
    }

    static Stream<String> formats() {
        return CLASS_NAMES_BY_FORMAT.keySet().stream();
    }
}
