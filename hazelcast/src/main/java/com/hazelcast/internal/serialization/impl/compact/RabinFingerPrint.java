/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

/**
 * A very collision-resistant fingerprint method used to create automatic schema id's for Compact format.
 */
public final class RabinFingerPrint {

    private static final long EMPTY = 0xc15d213aa4d7a795L;

    private static final long[] FP_TABLE = new long[256];
    static {
        for (int i = 0; i < 256; i++) {
            long fp = i;
            for (int j = 0; j < 8; j++) {
                fp = (fp >>> 1) ^ (EMPTY & -(fp & 1L));
            }
            FP_TABLE[i] = fp;
        }
    }

    private RabinFingerPrint() {
    }

    public static long fingerprint64(byte[] buf) {
        long fp = EMPTY;
        for (byte b : buf) {
            fp = (fp >>> 8) ^ FP_TABLE[(int) (fp ^ b) & 0xff];
        }
        return fp;
    }
}
