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

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.types.Code;
import org.bson.types.CodeWithScope;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.mongodb.impl.MongoUtilities.bsonTimestampToLocalDateTime;
import static java.util.Locale.ROOT;
import static java.util.stream.Collectors.toList;

/**
 * Utility class to help resolve BSON-Java types.
 */
final class BsonTypes {

    private static final Map<String, BsonType> BSON_NAME_TO_TYPE = generateBsonNameToBsonTypeMapping();
    private static final Map<Class<?>, BsonType> JAVA_TYPE_TO_BSON_TYPE = generateJavaClassToBsonTypeMapping();

    private BsonTypes() {
    }

    static BsonType resolveTypeFromJava(Object value) {
        Class<?> valueClass = value.getClass();
        BsonType bsonType = JAVA_TYPE_TO_BSON_TYPE.get(valueClass);

        // allow coerced types
        if (bsonType == null) {
            for (Class<?> candidate : JAVA_TYPE_TO_BSON_TYPE.keySet()) {
                if (candidate.isAssignableFrom(valueClass)) {
                    return JAVA_TYPE_TO_BSON_TYPE.get(candidate);
                }
            }
        }
        if (bsonType == null) {
            throw new IllegalArgumentException("BSON type " + valueClass + " is not known");
        }
        return bsonType;
    }

    static BsonType resolveTypeFromJavaClass(Class<?> clazz) {
        BsonType bsonType = JAVA_TYPE_TO_BSON_TYPE.get(clazz);
        if (bsonType == null) {
            throw new IllegalArgumentException("BSON type " + clazz + " is not known");
        }
        return bsonType;
    }

    static BsonType resolveTypeByName(String bsonTypeName) {
        BsonType bsonType = BSON_NAME_TO_TYPE.get(bsonTypeName.toLowerCase(ROOT));
        if (bsonType == null) {
            throw new IllegalArgumentException("BSON type " + bsonTypeName + " is not known");
        }
        return bsonType;
    }

    @SuppressWarnings("SpellCheckingInspection")
    private static Map<String, BsonType> generateBsonNameToBsonTypeMapping() {
        Map<String, BsonType> result = new HashMap<>();

        for (BsonType type : BsonType.values()) {
            result.put(type.name().toLowerCase(ROOT), type);
        }

        result.put("int", BsonType.INT32);
        result.put("long", BsonType.INT64);
        result.put("regex", BsonType.REGULAR_EXPRESSION);
        result.put("bool", BsonType.BOOLEAN);
        result.put("decimal", BsonType.DECIMAL128);
        result.put("minkey", BsonType.MIN_KEY);
        result.put("maxkey", BsonType.MAX_KEY);
        result.put("date", BsonType.DATE_TIME);
        result.put("objectid", BsonType.OBJECT_ID);
        result.put("object", BsonType.DOCUMENT);
        result.put("javascriptwithscope", BsonType.JAVASCRIPT_WITH_SCOPE);

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
        result.put(List.class, BsonType.ARRAY);
        result.put(Collection.class, BsonType.ARRAY);
        result.put(BigDecimal.class, BsonType.DECIMAL128);
        result.put(BsonDecimal128.class, BsonType.DECIMAL128);
        result.put(Decimal128.class, BsonType.DECIMAL128);
        result.put(BsonRegularExpression.class, BsonType.REGULAR_EXPRESSION);
        result.put(Boolean.class, BsonType.BOOLEAN);
        result.put(ObjectId.class, BsonType.OBJECT_ID);
        result.put(MinKey.class, BsonType.OBJECT_ID);
        result.put(MaxKey.class, BsonType.OBJECT_ID);
        result.put(Document.class, BsonType.DOCUMENT);
        result.put(Code.class, BsonType.JAVASCRIPT);
        result.put(CodeWithScope.class, BsonType.JAVASCRIPT_WITH_SCOPE);

        return result;
    }

    @SuppressWarnings({"checkstyle:ReturnCount", "checkstyle:RightCurly"})
    static Object unwrapSimpleWrappers(Object value) {
        if (value instanceof BsonBoolean) {
            return ((BsonBoolean) value).getValue();
        }
        if (value instanceof BsonInt32) {
            return ((BsonInt32) value).getValue();
        }
        if (value instanceof BsonInt64) {
            return ((BsonInt64) value).getValue();
        }
        if (value instanceof BsonDouble) {
            return ((BsonDouble) value).getValue();
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
        if (value instanceof BsonArray) {
            return ((BsonArray) value).getValues();
        }
        if (value instanceof BsonDateTime) {
            BsonDateTime v = (BsonDateTime) value;
            return LocalDateTime.from(Instant.ofEpochMilli(v.getValue()));
        }
        if (value instanceof BsonTimestamp) {
            return bsonTimestampToLocalDateTime((BsonTimestamp) value);
        }
        if (value instanceof BsonJavaScript) {
            return ((BsonJavaScript) value).getCode();
        }
        else if (value instanceof BsonJavaScriptWithScope) {
            return ((BsonJavaScriptWithScope) value).getCode();
        }
        if (value instanceof CodeWithScope) {
            return value;
        }
        if (value instanceof Code) {
            return ((Code) value).getCode();
        }
        return value;
    }


    /**
     * Gets {@link BsonType} from given Document being property descriptor from {@code jsonSchema}.
     */
    @SuppressWarnings("unchecked")
    static BsonType getBsonType(Document propertyDescription) {
        Object bsonType = propertyDescription.get("bsonType");
        if (bsonType instanceof String) {
            String bsonTypeName = (String) bsonType;
            return BsonTypes.resolveTypeByName(bsonTypeName);
        }
        if (bsonType instanceof Collection) {
            List<String> classes = ((Collection<String>) bsonType).stream()
                                                                  .filter(Objects::nonNull)
                                                                  .distinct()
                                                                  .collect(toList());
            if (classes.size() == 1) {
                return BsonTypes.resolveTypeByName(classes.get(0));
            } else {
                return BsonType.DOCUMENT;
            }
        } else if (bsonType == null) {
            Object enumValues = propertyDescription.get("enum");
            checkNotNull(enumValues, "either bsonType or enum should be provided");
            Collection<?> col = (Collection<?>) enumValues;
            List<? extends Class<?>> classes =
                    col.stream()
                       .filter(Objects::nonNull)
                       .map(Object::getClass)
                       .distinct()
                       .collect(toList());

            if (classes.size() != 1) {
                return BsonType.DOCUMENT;
            } else {
                return BsonTypes.resolveTypeFromJavaClass(classes.get(0));
            }
        }
        throw new UnsupportedOperationException("Cannot infer BSON type from schema");
    }
}
