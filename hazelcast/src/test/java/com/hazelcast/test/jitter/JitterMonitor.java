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

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyList;

class JitterMonitor {

    private static AtomicBoolean started = new AtomicBoolean();

    private static JitterRecorder jitterRecorder;

    static void ensureRunning() {
        if (started.compareAndSet(false, true)) {
            startMonitoringThread();
        }
    }

    static Iterable<Slot> getSlotsBetween(long startTime, long stopTime) {
        if (jitterRecorder == null) {
            return emptyList();
        }
        return jitterRecorder.getSlotsBetween(startTime, stopTime);
    }

    private static void startMonitoringThread() {
        jitterRecorder = new JitterRecorder();
        new JitterThread(jitterRecorder).start();
    }
}
