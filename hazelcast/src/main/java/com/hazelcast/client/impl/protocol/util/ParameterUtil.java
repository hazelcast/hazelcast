/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

public final class ParameterUtil {

    private static final int UTF8_MAX_BYTES_PER_CHAR = 3;

    private ParameterUtil() { }

    public static int calculateDataSize(String string) {
        return Bits.INT_SIZE_IN_BYTES + string.length() * UTF8_MAX_BYTES_PER_CHAR;
    }

    public static int calculateDataSize(Data data) {
        return calculateDataSize(data.toByteArray());
    }

    public static int calculateDataSize(byte[] bytes) {
        return Bits.INT_SIZE_IN_BYTES + bytes.length;
    }

    public static int calculateDataSize(Integer data) {
        return Bits.INT_SIZE_IN_BYTES;
    }

    public static int calculateDataSize(Boolean data) {
        return Bits.BOOLEAN_SIZE_IN_BYTES;
    }
}
