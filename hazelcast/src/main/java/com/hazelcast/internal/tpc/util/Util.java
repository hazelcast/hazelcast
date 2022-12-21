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

package com.hazelcast.internal.tpc.util;

import java.nio.ByteBuffer;
import java.time.Instant;

public class Util {
    private static final long NANOS_MULTIPLIER = 1_000_000_000;

    /**
     * Gets the number of nanoseconds from the Java epoch of 1970-01-01T00:00:00Z.
     *
     * @return the epoch time in nanoseconds.
     */
    public static long epochNanos() {

        //todo: litter
        Instant now = Instant.now();
        long seconds = now.getEpochSecond();
        long nanosFromSecond = now.getNano();

        return (seconds * NANOS_MULTIPLIER) + nanosFromSecond;
    }

    public static void put(ByteBuffer dst, ByteBuffer src) {
        if (src.remaining() <= dst.remaining()) {
            // there is enough space in the dst buffer to copy the src
            dst.put(src);
        } else {
            // there is not enough space in the dst buffer, so we need to
            // copy as much as we can.
            int srcOldLimit = src.limit();
            src.limit(src.position() + dst.remaining());
            dst.put(src);
            src.limit(srcOldLimit);
        }
    }
}
