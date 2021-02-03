/*
 * Original work Copyright 2015 Real Logic Ltd.
 * Modified work Copyright (c) 2015-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.collection;

import static com.hazelcast.internal.util.HashUtil.fastIntMix;
import static com.hazelcast.internal.util.HashUtil.fastLongMix;

/**
 * Hashcode functions for classes in this package.
 */
public final class Hashing {
    private Hashing() { }

    static int intHash(final int value, final int mask) {
        return fastIntMix(value) & mask;
    }

    static int longHash(final long value, final int mask) {
        return ((int) fastLongMix(value)) & mask;
    }

    static int evenLongHash(final long value, final int mask) {
        final int h = (int) fastLongMix(value);
        return h & mask & ~1;
    }

    static int hash(Object value, int mask) {
        return fastIntMix(value.hashCode()) & mask;
    }

    static int hashCode(long value) {
        // Used only for nominal Object.hashCode implementations, no mixing
        // required.
        return (int) (value ^ (value >>> Integer.SIZE));
    }

}
