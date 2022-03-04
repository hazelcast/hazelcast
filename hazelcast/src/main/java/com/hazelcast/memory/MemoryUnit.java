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

package com.hazelcast.memory;

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
        public long convert(long value, MemoryUnit m) {
            return m.toBytes(value);
        }

        public long toBytes(long value) {
            return value;
        }

        public long toKiloBytes(long value) {
            return divideByAndRoundToInt(value, K);
        }

        public long toMegaBytes(long value) {
            return divideByAndRoundToInt(value, M);
        }

        public long toGigaBytes(long value) {
            return divideByAndRoundToInt(value, G);
        }

        public String abbreviation() {
            return "B";
        }
    },

    /**
     * MemoryUnit in kilobytes
     */
    KILOBYTES {
        public long convert(long value, MemoryUnit m) {
            return m.toKiloBytes(value);
        }

        public long toBytes(long value) {
            return value * K;
        }

        public long toKiloBytes(long value) {
            return value;
        }

        public long toMegaBytes(long value) {
            return divideByAndRoundToInt(value, K);
        }

        public long toGigaBytes(long value) {
            return divideByAndRoundToInt(value, M);
        }

        public String abbreviation() {
            return "KB";
        }
    },

    /**
     * MemoryUnit in megabytes
     */
    MEGABYTES {
        public long convert(long value, MemoryUnit m) {
            return m.toMegaBytes(value);
        }

        public long toBytes(long value) {
            return value * M;
        }

        public long toKiloBytes(long value) {
            return value * K;
        }

        public long toMegaBytes(long value) {
            return value;
        }

        public long toGigaBytes(long value) {
            return divideByAndRoundToInt(value, K);
        }

        public String abbreviation() {
            return "MB";
        }
    },

    /**
     * MemoryUnit in gigabytes
     */
    GIGABYTES {
        public long convert(long value, MemoryUnit m) {
            return m.toGigaBytes(value);
        }

        public long toBytes(long value) {
            return value * G;
        }

        public long toKiloBytes(long value) {
            return value * M;
        }

        public long toMegaBytes(long value) {
            return value * K;
        }

        public long toGigaBytes(long value) {
            return value;
        }

        public String abbreviation() {
            return "GB";
        }
    };

    static final int POWER = 10;
    static final int K = 1 << POWER;
    static final int M = 1 << (POWER * 2);
    static final int G = 1 << (POWER * 3);

    public abstract long convert(long value, MemoryUnit m);

    public abstract long toBytes(long value);

    public abstract long toKiloBytes(long value);

    public abstract long toMegaBytes(long value);

    public abstract long toGigaBytes(long value);

    public abstract String abbreviation();
}
