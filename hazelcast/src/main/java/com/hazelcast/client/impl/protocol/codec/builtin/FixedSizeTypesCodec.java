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

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.config.BitmapIndexOptions.UniqueKeyTransformation;
import com.hazelcast.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType;
import com.hazelcast.config.IndexType;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.management.dto.ClientBwListEntryDTO;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.sql.SqlColumnType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("checkstyle:MethodCount")
public final class FixedSizeTypesCodec {

    public static final int BYTE_SIZE_IN_BYTES = Bits.BYTE_SIZE_IN_BYTES;
    public static final int SHORT_SIZE_IN_BYTES = Bits.SHORT_SIZE_IN_BYTES;
    public static final int INT_SIZE_IN_BYTES = Bits.INT_SIZE_IN_BYTES;
    public static final int LONG_SIZE_IN_BYTES = Bits.LONG_SIZE_IN_BYTES;
    public static final int FLOAT_SIZE_IN_BYTES = Bits.FLOAT_SIZE_IN_BYTES;
    public static final int DOUBLE_SIZE_IN_BYTES = Bits.DOUBLE_SIZE_IN_BYTES;
    public static final int BOOLEAN_SIZE_IN_BYTES = Bits.BOOLEAN_SIZE_IN_BYTES;
    public static final int UUID_SIZE_IN_BYTES = BOOLEAN_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES * 2;

    public static final int LOCAL_DATE_SIZE_IN_BYTES = INT_SIZE_IN_BYTES + BYTE_SIZE_IN_BYTES * 2;
    public static final int LOCAL_TIME_SIZE_IN_BYTES = BYTE_SIZE_IN_BYTES * 3 + INT_SIZE_IN_BYTES;
    public static final int LOCAL_DATE_TIME_SIZE_IN_BYTES = LOCAL_DATE_SIZE_IN_BYTES + LOCAL_TIME_SIZE_IN_BYTES;
    public static final int OFFSET_DATE_TIME_SIZE_IN_BYTES = LOCAL_DATE_TIME_SIZE_IN_BYTES + INT_SIZE_IN_BYTES;

    private FixedSizeTypesCodec() {
    }

    public static void encodeInt(byte[] buffer, int pos, int value) {
        Bits.writeIntL(buffer, pos, value);
    }

    public static int decodeInt(byte[] buffer, int pos) {
        return Bits.readIntL(buffer, pos);
    }

    public static void encodeInt(byte[] buffer, int pos, CacheEventType cacheEventType) {
        encodeInt(buffer, pos, cacheEventType.getType());
    }

    public static void encodeInt(byte[] buffer, int pos, IndexType indexType) {
        encodeInt(buffer, pos, indexType.getId());
    }

    public static void encodeInt(byte[] buffer, int pos, UniqueKeyTransformation uniqueKeyTransformation) {
        encodeInt(buffer, pos, uniqueKeyTransformation.getId());
    }

    public static void encodeInt(byte[] buffer, int pos, ExpiryPolicyType expiryPolicyType) {
        encodeInt(buffer, pos, expiryPolicyType.getId());
    }

    public static void encodeInt(byte[] buffer, int pos, ProtocolType protocolType) {
        encodeInt(buffer, pos, protocolType.getId());
    }

    public static void encodeInt(byte[] buffer, int pos, FieldKind fieldKind) {
        encodeInt(buffer, pos, fieldKind.getId());
    }

    public static void encodeInt(byte[] buffer, int pos, TimeUnit timeUnit) {
        int timeUnitId;
        if (TimeUnit.NANOSECONDS.equals(timeUnit)) {
            timeUnitId = 0;
        } else if (TimeUnit.MICROSECONDS.equals(timeUnit)) {
            timeUnitId = 1;
        } else if (TimeUnit.MILLISECONDS.equals(timeUnit)) {
            timeUnitId = 2;
        } else if (TimeUnit.SECONDS.equals(timeUnit)) {
            timeUnitId = 3;
        } else if (TimeUnit.MINUTES.equals(timeUnit)) {
            timeUnitId = 4;
        } else if (TimeUnit.HOURS.equals(timeUnit)) {
            timeUnitId = 5;
        } else if (TimeUnit.DAYS.equals(timeUnit)) {
            timeUnitId = 6;
        } else {
            timeUnitId = -1;
        }
        encodeInt(buffer, pos, timeUnitId);
    }

    public static void encodeInt(byte[] buffer, int pos, ClientBwListEntryDTO.Type clientBwListEntryType) {
        encodeInt(buffer, pos, clientBwListEntryType.getId());
    }

    public static void encodeInt(byte[] buffer, int pos, SqlColumnType columnType) {
        encodeInt(buffer, pos, columnType.getId());
    }

    public static void encodeShort(byte[] buffer, int pos, short value) {
        Bits.writeShortL(buffer, pos, value);
    }

    public static short decodeShort(byte[] buffer, int pos) {
        return Bits.readShortL(buffer, pos);
    }

    public static void encodeLong(byte[] buffer, int pos, long value) {
        Bits.writeLongL(buffer, pos, value);
    }

    public static long decodeLong(byte[] buffer, int pos) {
        return Bits.readLongL(buffer, pos);
    }

    public static void encodeFloat(byte[] buffer, int pos, float value) {
        encodeInt(buffer, pos, Float.floatToIntBits(value));
    }

    public static float decodeFloat(byte[] buffer, int pos) {
        return Float.intBitsToFloat(decodeInt(buffer, pos));
    }

    public static void encodeDouble(byte[] buffer, int pos, double value) {
        encodeLong(buffer, pos, Double.doubleToLongBits(value));
    }

    public static double decodeDouble(byte[] buffer, int pos) {
        return Double.longBitsToDouble(decodeLong(buffer, pos));
    }

    public static void encodeBoolean(byte[] buffer, int pos, boolean value) {
        buffer[pos] = (byte) (value ? 1 : 0);
    }

    public static boolean decodeBoolean(byte[] buffer, int pos) {
        return buffer[pos] == (byte) 1;
    }

    public static void encodeByte(byte[] buffer, int pos, byte value) {
        buffer[pos] = value;
    }

    public static byte decodeByte(byte[] buffer, int pos) {
        return buffer[pos];
    }

    public static void encodeUUID(byte[] buffer, int pos, UUID value) {
        boolean isNull = value == null;
        encodeBoolean(buffer, pos, isNull);
        if (isNull) {
            return;
        }
        long mostSigBits = value.getMostSignificantBits();
        long leastSigBits = value.getLeastSignificantBits();
        encodeLong(buffer, pos + BOOLEAN_SIZE_IN_BYTES, mostSigBits);
        encodeLong(buffer, pos + BOOLEAN_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES, leastSigBits);
    }

    public static UUID decodeUUID(byte[] buffer, int pos) {
        boolean isNull = decodeBoolean(buffer, pos);
        if (isNull) {
            return null;
        }
        long mostSigBits = decodeLong(buffer, pos + BOOLEAN_SIZE_IN_BYTES);
        long leastSigBits = decodeLong(buffer, pos + BOOLEAN_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES);
        return new UUID(mostSigBits, leastSigBits);
    }

    public static void encodeLocalDate(byte[] buffer, int pos, LocalDate value) {
        encodeInt(buffer, pos, value.getYear());
        encodeByte(buffer, pos + INT_SIZE_IN_BYTES, (byte) value.getMonthValue());
        encodeByte(buffer, pos + INT_SIZE_IN_BYTES + BYTE_SIZE_IN_BYTES, (byte) value.getDayOfMonth());
    }

    public static LocalDate decodeLocalDate(byte[] buffer, int pos) {
        int year = decodeInt(buffer, pos);
        int month = decodeByte(buffer, pos + INT_SIZE_IN_BYTES);
        int dayOfMonth = decodeByte(buffer, pos + INT_SIZE_IN_BYTES + BYTE_SIZE_IN_BYTES);

        return LocalDate.of(year, month, dayOfMonth);
    }

    public static void encodeLocalTime(byte[] buffer, int pos, LocalTime value) {
        encodeByte(buffer, pos, (byte) value.getHour());
        encodeByte(buffer, pos + BYTE_SIZE_IN_BYTES, (byte) value.getMinute());
        encodeByte(buffer, pos + BYTE_SIZE_IN_BYTES * 2, (byte) value.getSecond());
        encodeInt(buffer, pos + BYTE_SIZE_IN_BYTES * 3, value.getNano());
    }

    public static LocalTime decodeLocalTime(byte[] buffer, int pos) {
        int hour = decodeByte(buffer, pos);
        int minute = decodeByte(buffer, pos + BYTE_SIZE_IN_BYTES);
        int second = decodeByte(buffer, pos + BYTE_SIZE_IN_BYTES * 2);
        int nano = decodeInt(buffer, pos + BYTE_SIZE_IN_BYTES * 3);

        return LocalTime.of(hour, minute, second, nano);
    }

    public static void encodeLocalDateTime(byte[] buffer, int pos, LocalDateTime value) {
        encodeLocalDate(buffer, pos, value.toLocalDate());
        encodeLocalTime(buffer, pos + LOCAL_DATE_SIZE_IN_BYTES, value.toLocalTime());
    }

    public static LocalDateTime decodeLocalDateTime(byte[] buffer, int pos) {
        LocalDate date = decodeLocalDate(buffer, pos);
        LocalTime time = decodeLocalTime(buffer, pos + LOCAL_DATE_SIZE_IN_BYTES);

        return LocalDateTime.of(date, time);
    }

    public static void encodeOffsetDateTime(byte[] buffer, int pos, OffsetDateTime value) {
        encodeLocalDateTime(buffer, pos, value.toLocalDateTime());
        encodeInt(buffer, pos + LOCAL_DATE_TIME_SIZE_IN_BYTES, value.getOffset().getTotalSeconds());
    }

    public static OffsetDateTime decodeOffsetDateTime(byte[] buffer, int pos) {
        LocalDateTime dateTime = decodeLocalDateTime(buffer, pos);
        int offsetSeconds = decodeInt(buffer, pos + LOCAL_DATE_TIME_SIZE_IN_BYTES);

        return OffsetDateTime.of(dateTime, ZoneOffset.ofTotalSeconds(offsetSeconds));
    }
}
