/*
 * Original work Copyright 2015 Real Logic Ltd.
 * Modified work Copyright (c) 2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.collection;

/**
 * Hashcode calculation.
 */
public final class Hashing {
    private Hashing() { }

    public static int intHash(final int value, final int mask) {
        final int hash = value ^ (value >>> 16);
        return hash & mask;
    }

    public static int longHash(final long value, final int mask) {
        int hash = (int) value ^ (int) (value >>> 32);
        hash ^= (hash >>> 16);
        return hash & mask;
    }

    public static int evenLongHash(final long value, final int mask) {
        int hash = (int) value ^ (int) (value >>> 32);
        hash = (hash << 1) - (hash << 8);
        return hash & mask;
    }
}
