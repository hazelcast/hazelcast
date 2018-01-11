/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.jitter;

import static com.hazelcast.test.jitter.JitterRule.RESOLUTION_NANOS;
import static java.lang.Math.min;
import static java.util.concurrent.locks.LockSupport.parkNanos;

public class JitterThread extends Thread {

    private final JitterRecorder jitterRecorder;

    JitterThread(JitterRecorder jitterRecorder) {
        this.jitterRecorder = jitterRecorder;

        setName("JitterThread");
        setDaemon(true);
    }

    public void run() {
        long beforeNanos = System.nanoTime();
        long shortestHiccup = Long.MAX_VALUE;
        while (true) {
            long beforeMillis = System.currentTimeMillis();
            sleepNanos(RESOLUTION_NANOS);
            long after = System.nanoTime();
            long delta = after - beforeNanos;
            long currentHiccup = delta - RESOLUTION_NANOS;

            // subtract the shortest observed hiccups, as that's an inherit
            // cost of the measuring loop and OS scheduler imprecision
            shortestHiccup = min(shortestHiccup, currentHiccup);
            currentHiccup -= shortestHiccup;

            jitterRecorder.recordPause(beforeMillis, currentHiccup);
            beforeNanos = after;
        }
    }

    private void sleepNanos(long duration) {
        parkNanos(duration);
    }
}
