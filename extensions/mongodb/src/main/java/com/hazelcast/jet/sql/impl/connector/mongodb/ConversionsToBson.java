/*
 * Copyright 2025 Hazelcast Inc.
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
        if (value instanceof Integer integer) {
            return LocalDateTime.ofEpochSecond(integer, 0, UTC);
        }
        if (value instanceof Long longValue) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(longValue), UTC);
        }
        if (value instanceof CharSequence charSequence) {
            return LocalDateTime.parse(charSequence);
        }
        if (value instanceof BsonString bsonString) {
            return LocalDateTime.parse(bsonString.getValue());
        }
        if (value instanceof LocalDateTime localDateTime) {
            return new BsonDateTime(localDateTime.atZone(systemDefault()).withZoneSameInstant(UTC).toInstant().toEpochMilli());
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return new BsonDateTime(zonedDateTime.withZoneSameInstant(UTC).toInstant().toEpochMilli());
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return new BsonDateTime(offsetDateTime.atZoneSameInstant(UTC).toInstant().toEpochMilli());
        }
        return null;
    }

    private static BsonTimestamp convertToTimestamp(Object value) {
        if (value instanceof BsonTimestamp bsonTimestamp) {
            return bsonTimestamp;
        }
        if (value instanceof Number number) {
            return new BsonTimestamp(number.longValue());
        }
        if (value instanceof String string) {
            return new BsonTimestamp(Long.parseLong(string));
        }
        if (value instanceof BsonString bsonString) {
            return new BsonTimestamp(Long.parseLong(bsonString.getValue()));
        }
        if (value instanceof LocalDateTime localDateTime) {
            return new BsonTimestamp((int) localDateTime.atZone(systemDefault()).withZoneSameInstant(UTC).toEpochSecond(), 0);
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return new BsonTimestamp((int) zonedDateTime.withZoneSameInstant(UTC).toEpochSecond(), 0);
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return new BsonTimestamp((int) offsetDateTime.atZoneSameInstant(UTC).toEpochSecond(), 0);
        }
        return null;
    }

    private static ObjectId convertToObjectId(Object value) {
        if (value instanceof ObjectId objectId) {
            return objectId;
        }
        if (value instanceof String string) {
            return new ObjectId(string);
        }
        if (value instanceof Integer integer) {
            return new ObjectId(Integer.toHexString(integer));
        }
        if (value instanceof Long longValue) {
            return new ObjectId(Long.toHexString(longValue));
        }
        if (value instanceof BigDecimal bigDecimal) {
            return new ObjectId(bigDecimal.toBigInteger().toString(HEX_RADIX));
        }
        if (value instanceof BigInteger bigInteger) {
            return new ObjectId(bigInteger.toString(HEX_RADIX));
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
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        if (value instanceof String string) {
            return Boolean.parseBoolean(string);
        }
        if (value instanceof Number number) {
            return number.doubleValue() > 0;
        }
        return null;
    }

    private static Integer convertToInt(Object value) {
        if (value instanceof Integer integer) {
            return integer;
        }
        if (value instanceof BsonInt32 bsonInt32) {
            return bsonInt32.getValue();
        }
        if (value instanceof String string) {
            return Integer.parseInt(string);
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return null;
    }

    private static Long convertToLong(Object value) {
        if (value instanceof Long longValue) {
            return longValue;
        }
        if (value instanceof BsonInt64 bsonInt64) {
            return bsonInt64.getValue();
        }
        if (value instanceof String string) {
            return Long.parseLong(string);
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return null;
    }

    private static Double convertToDouble(Object value) {
        if (value instanceof Double doubleValue) {
            return doubleValue;
        }
        if (value instanceof BsonDouble bsonDouble) {
            return bsonDouble.getValue();
        }
        if (value instanceof String string) {
            return Double.parseDouble(string);
        }
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        return null;
    }

    private static Object convertToBigDecimal(Object value) {
        if (value instanceof BigDecimal || value instanceof BsonDecimal128 || value instanceof Decimal128) {
            return value;
        }
        if (value instanceof String string) {
            return new BigDecimal(string);
        }
        if (value instanceof Number) {
            return new BigDecimal(value.toString());
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Document convertToDocument(Object value) {
        if (value instanceof Document document) {
            return document;
        }
        if (value instanceof String string) {
            return Document.parse(string);
        }
        if (value instanceof BsonString bsonString) {
            return Document.parse(bsonString.getValue());
        }
        if (value instanceof HazelcastJsonValue hazelcastJsonValue) {
            return Document.parse(hazelcastJsonValue.getValue());
        }
        if (value instanceof Map) {
            return new Document((Map<String, Object>) value);
        }
        return null;
    }

    private static BsonJavaScript convertToJavaScript(Object value) {
        if (value instanceof BsonJavaScript bsonJavaScript) {
            return bsonJavaScript;
        }
        if (value instanceof String string) {
            return new BsonJavaScript(string);
        }
        return null;
    }

    private static BsonJavaScriptWithScope convertToJavaScriptWithScope(Object value) {
        if (value instanceof BsonJavaScriptWithScope bsonJavaScriptWithScope) {
            return bsonJavaScriptWithScope;
        }
        if (value instanceof Document doc) {
            return new BsonJavaScriptWithScope(doc.getString("code"), (BsonDocument) doc.get("scope"));
        }
        if (value instanceof BsonDocument doc) {
            return new BsonJavaScriptWithScope(doc.getString("code").toString(), (BsonDocument) doc.get("scope"));
        }

        return null;
    }

    private static String convertToString(Object value) {
        if (value instanceof String string) {
            return string;
        }
        if (value instanceof BsonString bsonString) {
            return bsonString.getValue();
        }
        if (value instanceof BsonJavaScript bsonJavaScript) {
            return bsonJavaScript.getCode();
        }
        if (value instanceof Document document) {
            return document.toJson();
        }
        if (value instanceof BsonDocument bsonDocument) {
            return bsonDocument.toJson();
        }
        if (value instanceof BsonJavaScriptWithScope bsonJavaScriptWithScope) {
            Document doc = new Document();
            doc.put("code", bsonJavaScriptWithScope.getCode());
            doc.put("scope", bsonJavaScriptWithScope.getScope());
            return doc.toJson();
        }
        return value.toString();
    }

    private static BsonRegularExpression convertToRegEx(Object value) {
        if (value instanceof BsonRegularExpression bsonRegularExpression) {
            return bsonRegularExpression;
        }
        if (value instanceof String string) {
            return new BsonRegularExpression(string);
        }
        return null;
    }

    private static MinKey convertToMinKey(Object value) {
        if (value instanceof MinKey minKey) {
            return minKey;
        }
        if (value instanceof BsonMinKey) {
            return new MinKey();
        }
        if (value instanceof String string && "Minkey".equalsIgnoreCase(string)) {
            return new MinKey();
        }
        return null;
    }

    private static MaxKey convertToMaxKey(Object value) {
        if (value instanceof MaxKey maxKey) {
            return maxKey;
        }
        if (value instanceof BsonMaxKey) {
            return new MaxKey();
        }
        if (value instanceof String string && "MaxKey".equalsIgnoreCase(string)) {
            return new MaxKey();
        }
        return null;
    }
}
