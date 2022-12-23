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

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;

public class Util {
    private static final long NANOS_MULTIPLIER = 1_000_000_000;
    public static final String OS = System.getProperty("os.name").toLowerCase();


    /**
     * Returns {@code true} if the system is Linux.
     *
     * @return {@code true} if the current system is Linux.
     */
    public static boolean isLinux() {
        return OS.contains("nux");
    }

    public static boolean is32bitJVM() {
        // sun.arch.data.model is available on Oracle, Zing and (most probably) IBM JVMs
        String architecture = System.getProperty("sun.arch.data.model");
        return architecture != null && architecture.equals("32");
    }

    /**
     * Ignores the given exception.
     *
     * @param t the exception to ignore
     */
    public static void ignore(Throwable t) {
    }

    public static void closeResource(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException ignore) {
        }
    }

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

    /**
     * Fast method of finding the next power of 2 greater than or equal to the supplied value.
     * <p>
     * If the value is &lt;= 0 then 1 will be returned.
     * <p>
     * This method is not suitable for {@link Integer#MIN_VALUE} or numbers greater than 2^30.
     *
     * @param value from which to search for next power of 2
     * @return The next power of 2 or the value itself if it is a power of 2
     */
    public static int nextPowerOfTwo(final int value) {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }
}
