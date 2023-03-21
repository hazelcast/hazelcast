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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Map;

import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.singletonList;

/**
 * Some type -> MongoDB type
 */
public final class ConversionsToBson {

    private static final int HEX_RADIX = 16;

    private ConversionsToBson() {
    }

    /**
     * Converts from user type to MongoDB-storable type.
     *
     * <p>
     * In other words, converts for INSERT/UPDATE/SINK INTO statements.
     */
    public static Object convertToBson(Object value, QueryDataType sqlType, BsonType bsonType) {
        if (value == null) {
            return null;
        }
        Object converted = null;

        switch (bsonType) {
            case OBJECT_ID:
                converted = convertToObjectId(value);
                break;
            case ARRAY:
                converted = convertToArray(value);
                break;
            case BOOLEAN:
                converted = convertToBoolean(value);
                break;
            case DATE_TIME:
                converted = convertToDateTime(value);
                break;
            case TIMESTAMP:
                converted = convertToTimestamp(value);
                break;
            case INT32:
                converted = convertToInt(value);
                break;
            case INT64:
                converted = convertToLong(value);
                break;
            case DOUBLE:
                converted = convertToDouble(value);
                break;
            case DECIMAL128:
                converted = convertToBigDecimal(value);
                break;
            case DOCUMENT:
                converted = convertToDocument(value);
                break;
            case JAVASCRIPT:
                converted = convertToJavaScript(value);
                break;
            case JAVASCRIPT_WITH_SCOPE:
                converted = convertToJavaScriptWithScope(value);
                break;
            case STRING:
                converted = convertToString(value);
                break;
            case REGULAR_EXPRESSION:
                converted = convertToRegEx(value);
                break;
            case MIN_KEY:
                converted = convertToMinKey(value);
                break;
            case MAX_KEY:
                converted = convertToMaxKey(value);
                break;
            default:
        }
        if (converted == null) {
            return sqlType.convert(value);
        } else {
            return converted;
        }
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private static Object convertToDateTime(Object value) {
        if (value instanceof BsonDateTime) {
            return value;
        }
        if (value instanceof Integer) {
            return LocalDateTime.ofEpochSecond((Integer) value, 0, UTC);
        }
        if (value instanceof Long) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) value), UTC);
        }
        if (value instanceof CharSequence) {
            return LocalDateTime.parse((CharSequence) value);
        }
        if (value instanceof BsonString) {
            return LocalDateTime.parse(((BsonString) value).getValue());
        }
        if (value instanceof LocalDateTime) {
            LocalDateTime v = (LocalDateTime) value;
            return new BsonDateTime(v.atZone(systemDefault()).withZoneSameInstant(UTC).toInstant().toEpochMilli());
        }
        if (value instanceof ZonedDateTime) {
            ZonedDateTime v = (ZonedDateTime) value;
            return new BsonDateTime(v.withZoneSameInstant(UTC).toInstant().toEpochMilli());
        }
        if (value instanceof OffsetDateTime) {
            OffsetDateTime v = (OffsetDateTime) value;
            return new BsonDateTime(v.atZoneSameInstant(UTC).toInstant().toEpochMilli());
        }
        return null;
    }

    private static BsonTimestamp convertToTimestamp(Object value) {
        if (value instanceof BsonTimestamp) {
            return (BsonTimestamp) value;
        }
        if (value instanceof Number) {
            return new BsonTimestamp(((Number) value).longValue());
        }
        if (value instanceof String) {
            return new BsonTimestamp(Long.parseLong((String) value));
        }
        if (value instanceof BsonString) {
            return new BsonTimestamp(Long.parseLong(((BsonString) value).getValue()));
        }
        if (value instanceof LocalDateTime) {
            LocalDateTime v = (LocalDateTime) value;
            return new BsonTimestamp((int) v.atZone(systemDefault()).withZoneSameInstant(UTC).toEpochSecond(), 0);
        }
        if (value instanceof ZonedDateTime) {
            ZonedDateTime v = (ZonedDateTime) value;
            return new BsonTimestamp((int) v.withZoneSameInstant(UTC).toEpochSecond(), 0);
        }
        if (value instanceof OffsetDateTime) {
            OffsetDateTime v = (OffsetDateTime) value;
            return new BsonTimestamp((int) v.atZoneSameInstant(UTC).toEpochSecond(), 0);
        }
        return null;
    }

    private static ObjectId convertToObjectId(Object value) {
        if (value instanceof ObjectId) {
            return (ObjectId) value;
        }
        if (value instanceof String) {
            return new ObjectId((String) value);
        }
        if (value instanceof Integer) {
            return new ObjectId(Integer.toHexString((Integer) value));
        }
        if (value instanceof Long) {
            return new ObjectId(Long.toHexString((Long) value));
        }
        if (value instanceof BigDecimal) {
            BigDecimal v = (BigDecimal) value;
            return new ObjectId(v.toBigInteger().toString(HEX_RADIX));
        }
        if (value instanceof BigInteger) {
            BigInteger v = (BigInteger) value;
            return new ObjectId(v.toString(HEX_RADIX));
        }
        return null;
    }

    private static Collection<?> convertToArray(Object value) {
        if (value instanceof Collection) {
            return (Collection<?>) value;
        }

        return singletonList(value);
    }

    private static Boolean convertToBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue() > 0;
        }
        return null;
    }

    private static Integer convertToInt(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof BsonInt32) {
            return ((BsonInt32) value).getValue();
        }
        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return null;
    }

    private static Long convertToLong(Object value) {
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof BsonInt64) {
            return ((BsonInt64) value).getValue();
        }
        if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return null;
    }

    private static Double convertToDouble(Object value) {
        if (value instanceof Double) {
            return (Double) value;
        }
        if (value instanceof BsonDouble) {
            return ((BsonDouble) value).getValue();
        }
        if (value instanceof String) {
            return Double.parseDouble((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return null;
    }

    private static Object convertToBigDecimal(Object value) {
        if (value instanceof BigDecimal || value instanceof BsonDecimal128 || value instanceof Decimal128) {
            return value;
        }
        if (value instanceof String) {
            return new BigDecimal((String) value);
        }
        if (value instanceof Number) {
            return new BigDecimal(value.toString());
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Document convertToDocument(Object value) {
        if (value instanceof Document) {
            return (Document) value;
        }
        if (value instanceof String) {
            return Document.parse((String) value);
        }
        if (value instanceof BsonString) {
            return Document.parse(((BsonString) value).getValue());
        }
        if (value instanceof HazelcastJsonValue) {
            return Document.parse(((HazelcastJsonValue) value).getValue());
        }
        if (value instanceof Map) {
            return new Document((Map<String, Object>) value);
        }
        return null;
    }

    private static BsonJavaScript convertToJavaScript(Object value) {
        if (value instanceof BsonJavaScript) {
            return (BsonJavaScript) value;
        }
        if (value instanceof String) {
            return new BsonJavaScript((String) value);
        }
        return null;
    }

    private static BsonJavaScriptWithScope convertToJavaScriptWithScope(Object value) {
        if (value instanceof BsonJavaScriptWithScope) {
            return (BsonJavaScriptWithScope) value;
        }
        if (value instanceof Document) {
            Document doc = (Document) value;
            return new BsonJavaScriptWithScope(doc.getString("code"), (BsonDocument) doc.get("scope"));
        }
        if (value instanceof BsonDocument) {
            BsonDocument doc = (BsonDocument) value;
            return new BsonJavaScriptWithScope(doc.getString("code").toString(), (BsonDocument) doc.get("scope"));
        }

        return null;
    }

    private static String convertToString(Object value) {
        if (value instanceof String) {
            return (String) value;
        }
        if (value instanceof BsonString) {
            return ((BsonString) value).getValue();
        }
        if (value instanceof BsonJavaScript) {
            return ((BsonJavaScript) value).getCode();
        }
        if (value instanceof Document) {
            return ((Document) value).toJson();
        }
        if (value instanceof BsonDocument) {
            return ((BsonDocument) value).toJson();
        }
        if (value instanceof BsonJavaScriptWithScope) {
            BsonJavaScriptWithScope v = (BsonJavaScriptWithScope) value;
            Document doc = new Document();
            doc.put("code", v.getCode());
            doc.put("scope", v.getScope());
            return doc.toJson();
        }
        return value.toString();
    }

    private static BsonRegularExpression convertToRegEx(Object value) {
        if (value instanceof BsonRegularExpression) {
            return (BsonRegularExpression) value;
        }
        if (value instanceof String) {
            return new BsonRegularExpression((String) value);
        }
        return null;
    }

    private static MinKey convertToMinKey(Object value) {
        if (value instanceof MinKey) {
            return (MinKey) value;
        }
        if (value instanceof BsonMinKey) {
            return new MinKey();
        }
        if (value instanceof String && "Minkey".equalsIgnoreCase((String) value)) {
            return new MinKey();
        }
        return null;
    }

    private static MaxKey convertToMaxKey(Object value) {
        if (value instanceof MaxKey) {
            return (MaxKey) value;
        }
        if (value instanceof BsonMaxKey) {
            return new MaxKey();
        }
        if (value instanceof String && "MaxKey".equalsIgnoreCase((String) value)) {
            return new MaxKey();
        }
        return null;
    }
}
