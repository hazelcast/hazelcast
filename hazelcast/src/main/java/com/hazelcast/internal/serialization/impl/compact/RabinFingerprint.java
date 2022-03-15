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

package com.hazelcast.internal.serialization.impl.compact;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

import static com.hazelcast.internal.nio.Bits.NULL_ARRAY_LENGTH;

/**
 * A very collision-resistant fingerprint method used to create automatic
 * schema ids for the Compact format.
 */
public final class RabinFingerprint {

    private static final long INIT = 0xc15d213aa4d7a795L;
    private static final long[] FP_TABLE = new long[256];

    static {
        for (int i = 0; i < 256; i++) {
            long fp = i;
            for (int j = 0; j < 8; j++) {
                fp = (fp >>> 1) ^ (INIT & -(fp & 1L));
            }
            FP_TABLE[i] = fp;
        }
    }

    private RabinFingerprint() {
    }

    /**
     * Calculates the fingerprint of the schema from its type name and fields.
     */
    public static long fingerprint64(Schema schema) {
        long fingerPrint = fingerprint64(INIT, schema.getTypeName());
        fingerPrint = fingerprint64(fingerPrint, schema.getFieldCount());
        for (FieldDescriptor descriptor : schema.getFields()) {
            fingerPrint = fingerprint64(fingerPrint, descriptor.getFieldName());
            fingerPrint = fingerprint64(fingerPrint, descriptor.getKind().getId());
        }
        return fingerPrint;
    }

    // Package-private for tests
    static long fingerprint64(byte[] buf) {
        long fp = INIT;
        for (byte b : buf) {
            fp = fingerprint64(fp, b);
        }
        return fp;
    }

    private static long fingerprint64(long fp, byte b) {
        return (fp >>> 8) ^ FP_TABLE[(int) (fp ^ b) & 0xff];
    }

    private static long fingerprint64(long fp, @Nullable String value) {
        if (value == null) {
            return fingerprint64(fp, NULL_ARRAY_LENGTH);
        }
        byte[] utf8Bytes = value.getBytes(StandardCharsets.UTF_8);
        fp = fingerprint64(fp, utf8Bytes.length);
        for (byte utf8Byte : utf8Bytes) {
            fp = fingerprint64(fp, utf8Byte);
        }
        return fp;
    }

    /**
     * FingerPrint of a little endian representation of an integer.
     */
    private static long fingerprint64(long fp, int v) {
        fp = fingerprint64(fp, (byte) ((v) & 0xFF));
        fp = fingerprint64(fp, (byte) ((v >>> 8) & 0xFF));
        fp = fingerprint64(fp, (byte) ((v >>> 16) & 0xFF));
        fp = fingerprint64(fp, (byte) ((v >>> 24) & 0xFF));
        return fp;
    }
}
