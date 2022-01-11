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


package com.hazelcast.jet.sql.impl.expression.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonPrimitive;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;

public final class JsonCreationUtil {
    private static final Gson SERIALIZER = new GsonBuilder()
            .disableHtmlEscaping()
            .serializeNulls()
            .create();

    private JsonCreationUtil() { }

    public static String serializeValue(final Object value) {
        if (value == null) {
            return "null";
        }

        if (value instanceof HazelcastJsonValue) {
            return value.toString();
        }

        final Converter converter = Converters.getConverter(value.getClass());
        final Object result;
        switch (converter.getTypeFamily()) {
            case TIME:
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
                result = converter.asVarchar(value);
                break;
            default:
                result = value;
        }

        return SERIALIZER.toJson(result);
    }

    public static String serializeString(String string) {
        if (string == null) {
            return "null";
        }

        return SERIALIZER.toJson(new JsonPrimitive(string));
    }
}
