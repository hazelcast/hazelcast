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

import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Utility class to help resolve BSON-Java types.
 */
final class BsonTypes {

    private static final Map<String, BsonType> BSON_NAME_TO_TYPE = generateBsonNameToBsonTypeMapping();
    private static final Map<Class<?>, BsonType> JAVA_TYPE_TO_BSON_TYPE = generateJavaClassToBsonTypeMapping();

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

    private static Map<String, BsonType> generateBsonNameToBsonTypeMapping() {
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
        result.put("date", BsonType.DATE_TIME);

        return result;
    }

    private static Map<Class<?>, BsonType> generateJavaClassToBsonTypeMapping() {
        Map<Class<?>, BsonType> result = new HashMap<>();

        result.put(Integer.class, BsonType.INT32);
        result.put(BsonInt32.class, BsonType.INT32);
        result.put(BsonInt64.class, BsonType.INT64);
        result.put(int.class, BsonType.INT32);
        result.put(Long.class, BsonType.INT64);
        result.put(long.class, BsonType.INT64);
        result.put(Double.class, BsonType.DOUBLE);
        result.put(double.class, BsonType.DOUBLE);
        result.put(float.class, BsonType.DOUBLE);
        result.put(Float.class, BsonType.DOUBLE);
        result.put(BsonDouble.class, BsonType.DOUBLE);
        result.put(BsonDateTime.class, BsonType.DATE_TIME);
        result.put(Date.class, BsonType.DATE_TIME);
        result.put(BsonTimestamp.class, BsonType.TIMESTAMP);
        result.put(Timestamp.class, BsonType.TIMESTAMP);
        result.put(String.class, BsonType.STRING);
        result.put(Object[].class, BsonType.ARRAY);
        result.put(BigDecimal.class, BsonType.DECIMAL128);
        result.put(BsonDecimal128.class, BsonType.DECIMAL128);
        result.put(Boolean.class, BsonType.BOOLEAN);
        result.put(ObjectId.class, BsonType.OBJECT_ID);

        return result;
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    static Object unwrap(Object value) {
        if (value instanceof BsonDateTime) {
            BsonDateTime v = (BsonDateTime) value;
            return LocalDateTime.from(new Date(v.getValue()).toInstant());
        }
        if (value instanceof BsonString) {
            return value.toString();
        }
        if (value instanceof BsonDecimal128) {
            BsonDecimal128 v = (BsonDecimal128) value;
            return new BigDecimal(v.toString());
        }
        if (value instanceof Decimal128) {
            Decimal128 v = (Decimal128) value;
            return new BigDecimal(v.toString());
        }
        if (value instanceof BsonDouble) {
            return ((BsonDouble) value).getValue();
        }
        if (value instanceof BsonInt32) {
            return ((BsonInt32) value).getValue();
        }
        if (value instanceof BsonInt64) {
            return ((BsonInt64) value).getValue();
        }
        if (value instanceof BsonBoolean) {
            return ((BsonBoolean) value).getValue();
        }
        return value;
    }
}
