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
package com.hazelcast.jet.sql.impl.connector.mongodb;

import org.bson.BsonDecimal128;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Utility class to help resolve BSON-Java types.
 */
final class BsonTypes {

    private static final Map<String, BsonType> BSON_NAME_TO_TYPE = generateTypes();
    private static final Map<Class<?>, BsonType> JAVA_TYPE_TO_BSON_TYPE = generateTypesFromJava();

    private BsonTypes() {
    }

    static BsonType resolveTypeFromJava(Object value) {
        BsonType bsonType = JAVA_TYPE_TO_BSON_TYPE.get(value.getClass());
        if (bsonType == null) {
            throw new IllegalArgumentException("BSON type " + value.getClass() + " is not known");
        }
        return bsonType;
    }

    static BsonType resolveTypeByName(String bsonTypeName) {
        BsonType bsonType = BSON_NAME_TO_TYPE.get(bsonTypeName);
        if (bsonType == null) {
            throw new IllegalArgumentException("BSON type " + bsonTypeName + " is not known");
        }
        return bsonType;
    }

    private static Map<String, BsonType> generateTypes() {
        Map<String, BsonType> result = new HashMap<>();

        for (BsonType type : BsonType.values()) {
            result.put(type.name().toLowerCase(Locale.ROOT), type);
        }
        result.put("int", BsonType.INT32);
        result.put("long", BsonType.INT64);
        result.put("regex", BsonType.REGULAR_EXPRESSION);
        result.put("bool", BsonType.BOOLEAN);
        result.put("decimal", BsonType.DECIMAL128);
        result.put("minKey", BsonType.MIN_KEY);
        result.put("maxKey", BsonType.MAX_KEY);

        return result;
    }

    private static Map<Class<?>, BsonType> generateTypesFromJava() {
        Map<Class<?>, BsonType> result = new HashMap<>();

        result.put(Integer.class, BsonType.INT32);
        result.put(BsonInt32.class, BsonType.INT32);
        result.put(BsonInt64.class, BsonType.INT64);
        result.put(int.class, BsonType.INT32);
        result.put(Long.class, BsonType.INT64);
        result.put(long.class, BsonType.INT64);
        result.put(Double.class, BsonType.DOUBLE);
        result.put(double.class, BsonType.DOUBLE);
        result.put(BsonDouble.class, BsonType.DOUBLE);
        result.put(BsonTimestamp.class, BsonType.TIMESTAMP);
        result.put(String.class, BsonType.STRING);
        result.put(Object[].class, BsonType.ARRAY);
        result.put(BigDecimal.class, BsonType.DECIMAL128);
        result.put(BsonDecimal128.class, BsonType.DECIMAL128);
        result.put(Boolean.class, BsonType.BOOLEAN);
        result.put(ObjectId.class, BsonType.OBJECT_ID);

        return result;
    }
}
