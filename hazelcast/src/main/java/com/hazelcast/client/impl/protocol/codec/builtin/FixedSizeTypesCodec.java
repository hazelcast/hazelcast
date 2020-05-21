/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public final class FixedSizeTypesCodec {

    public static final int BYTE_SIZE_IN_BYTES = Bits.BYTE_SIZE_IN_BYTES;
    public static final int LONG_SIZE_IN_BYTES = Bits.LONG_SIZE_IN_BYTES;
    public static final int INT_SIZE_IN_BYTES = Bits.INT_SIZE_IN_BYTES;
    public static final int BOOLEAN_SIZE_IN_BYTES = Bits.BOOLEAN_SIZE_IN_BYTES;
    public static final int UUID_SIZE_IN_BYTES = Bits.BOOLEAN_SIZE_IN_BYTES + Bits.LONG_SIZE_IN_BYTES * 2;

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

    public static void encodeInteger(byte[] buffer, int pos, Integer value) {
        Bits.writeIntL(buffer, pos, value);
    }

    public static Integer decodeInteger(byte[] buffer, int pos) {
        return Bits.readIntL(buffer, pos);
    }

    public static void encodeLong(byte[] buffer, int pos, long value) {
        Bits.writeLongL(buffer, pos, value);
    }

    public static long decodeLong(byte[] buffer, int pos) {
        return Bits.readLongL(buffer, pos);
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

}
