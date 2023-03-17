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
import org.bson.BsonDocument;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.JSON;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.OBJECT;
import static java.time.ZoneId.systemDefault;
import static java.time.ZoneOffset.UTC;

public final class ConversionsFromBson {

    public static final int OCT_RADIX = 16;

    private ConversionsFromBson() {
    }

    /**
     * Converts from MongoDB-read value.
     * <p>
     * In other words, converts for SELECT statements.
     */
    @Nullable
    @SuppressWarnings("checkstyle:RightCurly")
    public static Object convertFromBson(@Nullable Object toConvert, @Nonnull QueryDataType sqlType) {
        if (toConvert == null) {
            return null;
        }
        // common unwrapping to standard types
        Object value = BsonTypes.unwrapSimpleWrappers(toConvert);
        // todo check if necessary
        if (sqlType.getTypeFamily() == JSON) {
            if (value instanceof String) {
                return new HazelcastJsonValue((String) value);
            }
            if (value instanceof Document) {
                Document doc = (Document) value;
                return new HazelcastJsonValue(doc.toJson());
            }
            if (value instanceof BsonDocument) {
                BsonDocument doc = (BsonDocument) value;
                return new HazelcastJsonValue(doc.toJson());
            }
        }


        if (value instanceof ObjectId) {
            value = convertObjectId((ObjectId) value, sqlType);
        }
        else if (toConvert instanceof BsonDateTime) {
            value = convertDateTime((BsonDateTime) toConvert, sqlType);
        }
        else if (toConvert instanceof BsonTimestamp) {
            value = convertTimestamp((BsonTimestamp) toConvert, sqlType);
        }
        else if (toConvert instanceof BsonMinKey || toConvert instanceof BsonMaxKey
                || toConvert instanceof MinKey || toConvert instanceof MaxKey) {
            if (sqlType.getTypeFamily() != OBJECT) {
                value = toConvert.toString();
            }
        }
        else if (toConvert instanceof BsonJavaScript) {
            value = ((BsonJavaScript) value).getCode();
        }
        else if (toConvert instanceof BsonJavaScriptWithScope) {
            value = ((BsonJavaScript) value).getCode();
        }
        else if (toConvert instanceof Date) {
            Date date = (Date) toConvert;
            value = LocalDateTime.from(date.toInstant().atZone(ZoneId.systemDefault()));
        }
        return sqlType.convert(value);
    }

    private static Object convertObjectId(@Nonnull ObjectId value, QueryDataType resultType) {
        if (resultType.equals(QueryDataType.VARCHAR)) {
            return value.toHexString();
        } else if (resultType.equals(QueryDataType.OBJECT)) {
            return value;
        } else if (resultType.equals(QueryDataType.INT)) {
            return Long.parseLong(value.toHexString(), OCT_RADIX);
        } else if (resultType.equals(QueryDataType.BIGINT)) {
            return new BigDecimal(Long.parseLong(value.toHexString(), OCT_RADIX));
        }
        return null;
    }
    @SuppressWarnings("checkstyle:ReturnCount")
    private static Object convertDateTime(@Nonnull BsonDateTime value, QueryDataType resultType) {
        LocalDateTime dateTime = LocalDateTime.from(Instant.ofEpochMilli(value.getValue()));
        if (resultType.equals(QueryDataType.DATE)) {
            return dateTime.toLocalDate();
        }
        if (resultType.equals(QueryDataType.TIME)) {
            return dateTime.toLocalTime();
        }
        if (resultType.equals(QueryDataType.TIMESTAMP)) {
            return dateTime;
        }
        if (resultType.equals(QueryDataType.TIMESTAMP_WITH_TZ_DATE)) {
            return OffsetDateTime.from(dateTime);
        }
        if (resultType.equals(QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME)) {
            return OffsetDateTime.from(dateTime).toZonedDateTime();
        }
        if (resultType.equals(QueryDataType.VARCHAR)) {
        return dateTime.toString();
        } else if (resultType.equals(QueryDataType.OBJECT)) {
            return value;
        } else if (resultType.equals(QueryDataType.INT)) {
            return value.getValue();
        } else if (resultType.equals(QueryDataType.BIGINT)) {
            return new BigDecimal(value.getValue());
        }
        return null;
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private static Object convertTimestamp(BsonTimestamp value, QueryDataType resultType) {
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(value.getTime(), 0, UTC).atZone(systemDefault())
                .toLocalDateTime();
        if (resultType.equals(QueryDataType.DATE)) {
            return dateTime.toLocalDate();
        }
        if (resultType.equals(QueryDataType.TIME)) {
            return dateTime.toLocalTime();
        }
        if (resultType.equals(QueryDataType.TIMESTAMP)) {
            return dateTime;
        }
        if (resultType.equals(QueryDataType.TIMESTAMP_WITH_TZ_DATE)) {
            return OffsetDateTime.from(dateTime);
        }
        if (resultType.equals(QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME)) {
            return OffsetDateTime.from(dateTime).toZonedDateTime();
        }
        if (resultType.equals(QueryDataType.VARCHAR)) {
            return dateTime.toString();
        } else if (resultType.equals(QueryDataType.OBJECT)) {
            return value;
        } else if (resultType.equals(QueryDataType.INT)) {
            return value.getValue();
        } else if (resultType.equals(QueryDataType.BIGINT)) {
            return new BigDecimal(value.getValue());
        }
        return null;
    }

}
