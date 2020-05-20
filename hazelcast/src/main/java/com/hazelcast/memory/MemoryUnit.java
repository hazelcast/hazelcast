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

package com.hazelcast.memory;

import com.hazelcast.internal.JavaDocClear;

import static com.hazelcast.internal.util.QuickMath.divideByAndRoundToInt;

/**
 * MemoryUnit represents memory size at a given unit of
 * granularity and provides utility methods to convert across units.
 *
 * @see MemorySize
 * @since 3.4
 */
public enum MemoryUnit {

    /**
     * MemoryUnit in bytes
     */
    BYTES {
        @JavaDocClear
        public long convert(long value, MemoryUnit m) {
            return m.toBytes(value);
        }

        @JavaDocClear
        public long toBytes(long value) {
            return value;
        }

        @JavaDocClear
        public long toKiloBytes(long value) {
            return divideByAndRoundToInt(value, K);
        }

        @JavaDocClear
        public long toMegaBytes(long value) {
            return divideByAndRoundToInt(value, M);
        }

        @JavaDocClear
        public long toGigaBytes(long value) {
            return divideByAndRoundToInt(value, G);
        }
    },

    /**
     * MemoryUnit in kilobytes
     */
    KILOBYTES {
        @JavaDocClear
        public long convert(long value, MemoryUnit m) {
            return m.toKiloBytes(value);
        }

        @JavaDocClear
        public long toBytes(long value) {
            return value * K;
        }

        @JavaDocClear
        public long toKiloBytes(long value) {
            return value;
        }

        @JavaDocClear
        public long toMegaBytes(long value) {
            return divideByAndRoundToInt(value, K);
        }

        @JavaDocClear
        public long toGigaBytes(long value) {
            return divideByAndRoundToInt(value, M);
        }
    },

    /**
     * MemoryUnit in megabytes
     */
    MEGABYTES {
        @JavaDocClear
        public long convert(long value, MemoryUnit m) {
            return m.toMegaBytes(value);
        }

        @JavaDocClear
        public long toBytes(long value) {
            return value * M;
        }

        @JavaDocClear
        public long toKiloBytes(long value) {
            return value * K;
        }

        @JavaDocClear
        public long toMegaBytes(long value) {
            return value;
        }

        @JavaDocClear
        public long toGigaBytes(long value) {
            return divideByAndRoundToInt(value, K);
        }
    },

    /**
     * MemoryUnit in gigabytes
     */
    GIGABYTES {
        @JavaDocClear
        public long convert(long value, MemoryUnit m) {
            return m.toGigaBytes(value);
        }

        @JavaDocClear
        public long toBytes(long value) {
            return value * G;
        }

        @JavaDocClear
        public long toKiloBytes(long value) {
            return value * M;
        }

        @JavaDocClear
        public long toMegaBytes(long value) {
            return value * K;
        }

        @JavaDocClear
        public long toGigaBytes(long value) {
            return value;
        }
    };

    static final int POWER = 10;
    static final int K = 1 << POWER;
    static final int M = 1 << (POWER * 2);
    static final int G = 1 << (POWER * 3);

    @JavaDocClear
    public abstract long convert(long value, MemoryUnit m);

    @JavaDocClear
    public abstract long toBytes(long value);

    @JavaDocClear
    public abstract long toKiloBytes(long value);

    @JavaDocClear
    public abstract long toMegaBytes(long value);

    @JavaDocClear
    public abstract long toGigaBytes(long value);
}
