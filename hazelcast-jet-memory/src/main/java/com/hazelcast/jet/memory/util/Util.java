/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.memory.util;

import com.hazelcast.util.QuickMath;

import java.util.concurrent.ExecutionException;

/**
 * Utilite methods.
 */
public final class Util {
    public static final byte BYTE_0 = 0;
    public static final byte BYTE_1 = 1;

    private Util() {
    }

    public static void assertPositiveInt(long value) {
        assert value > 0 && value <= Integer.MAX_VALUE;
    }

    public static boolean isPositivePowerOfTwo(long x) {
        return x > 0 && QuickMath.isPowerOfTwo(x);
    }

    public static RuntimeException rethrow(Throwable e) {
        while (e instanceof ExecutionException && e.getCause() != null) {
            e = e.getCause();
        }
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        throw new RuntimeException(e);
    }
}
