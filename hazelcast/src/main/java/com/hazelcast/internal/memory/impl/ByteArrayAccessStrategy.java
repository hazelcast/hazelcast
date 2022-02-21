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

package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.ByteAccessStrategy;

/**
 * Implementation of {@link ByteAccessStrategy} for Java byte arrays.
 */
public final class ByteArrayAccessStrategy implements ByteAccessStrategy<byte[]> {

    public static final ByteArrayAccessStrategy INSTANCE = new ByteArrayAccessStrategy();

    private ByteArrayAccessStrategy() {
    }

    @Override
    public byte getByte(byte[] array, long offset) {
        assertFitsInt(offset);
        return array[(int) offset];
    }

    @Override
    public void putByte(byte[] array, long offset, byte x) {
        assertFitsInt(offset);
        array[(int) offset] = x;
    }

    private static void assertFitsInt(long arg) {
        assert arg >= 0 && arg <= Integer.MAX_VALUE : "argument outside of int range: " + arg;
    }
}
