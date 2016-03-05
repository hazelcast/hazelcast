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

package com.hazelcast.jet.memory.impl.util;

import java.io.File;
import java.util.concurrent.ExecutionException;

public final class Util {
    public static final byte ZERO = (byte) 0;

    public static final byte BYTE = (byte) 1;

    private Util() {
    }

    public static void assertPositiveInt(long value) {
        assert value > 0 && value <= Integer.MAX_VALUE;
    }

    public static boolean isPositivePowerOfTwo(long x) {
        return (x > 0) && (Long.bitCount(x) == 1);
    }

    public static RuntimeException reThrow(Throwable e) {
        if (e instanceof ExecutionException) {
            if (e.getCause() != null) {
                throw reThrow(e.getCause());
            } else {
                throw new RuntimeException(e);
            }
        }

        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }

        return new RuntimeException(e);
    }

    public static void deleteFile(File file) {
        if ((file != null) && (file.exists())) {
            if (!file.delete()) {
                throw new IllegalStateException("Can't delete file " + file.getAbsolutePath());
            }
        }
    }

}
