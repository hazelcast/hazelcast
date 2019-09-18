/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.Bits;

import java.util.UUID;

public final class FixedSizeTypesCodec {

    public static final int BYTE_SIZE_IN_BYTES = Bits.BYTE_SIZE_IN_BYTES;
    public static final int LONG_SIZE_IN_BYTES = Bits.LONG_SIZE_IN_BYTES;
    public static final int INT_SIZE_IN_BYTES = Bits.INT_SIZE_IN_BYTES;
    public static final int BOOLEAN_SIZE_IN_BYTES = Bits.BOOLEAN_SIZE_IN_BYTES;
    public static final int UUID_SIZE_IN_BYTES = Bits.LONG_SIZE_IN_BYTES * 2;

    private FixedSizeTypesCodec() {
    }

    public static void encodeInt(byte[] buffer, int pos, int value) {
        Bits.writeIntL(buffer, pos, value);
    }

    public static int decodeInt(byte[] buffer, int pos) {
        return Bits.readIntL(buffer, pos);
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
        long mostSigBits = value.getMostSignificantBits();
        long leastSigBits = value.getLeastSignificantBits();
        encodeLong(buffer, pos, mostSigBits);
        encodeLong(buffer, pos + LONG_SIZE_IN_BYTES, leastSigBits);
    }

    public static UUID decodeUUID(byte[] buffer, int pos) {
        long mostSigBits = decodeLong(buffer, pos);
        long leastSigBits = decodeLong(buffer, pos + LONG_SIZE_IN_BYTES);
        return new UUID(mostSigBits, leastSigBits);
    }

}
