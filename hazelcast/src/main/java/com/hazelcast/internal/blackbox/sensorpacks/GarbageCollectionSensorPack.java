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

package com.hazelcast.internal.blackbox.sensorpacks;


import com.hazelcast.internal.blackbox.Blackbox;
import com.hazelcast.internal.blackbox.SensorInput;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A Sensor pack for exposing {@link GarbageCollectorMXBean} sensors.
 */
public final class GarbageCollectionSensorPack {

    private static final Set<String> YOUNG_GC = new HashSet<String>(3);
    private static final Set<String> OLD_GC = new HashSet<String>(3);
    private static final int PUBLISH_FREQUENCY_SECONDS = 1;

    static {
        YOUNG_GC.add("PS Scavenge");
        YOUNG_GC.add("ParNew");
        YOUNG_GC.add("G1 Young Generation");

        OLD_GC.add("PS MarkSweep");
        OLD_GC.add("ConcurrentMarkSweep");
        OLD_GC.add("G1 Old Generation");
    }

    private GarbageCollectionSensorPack() {
    }

    /**
     * Registers all the sensors in this sensor pack.
     *
     * @param blackbox the Blackbox the sensors are registered on.
     */
    public static void register(Blackbox blackbox) {
        checkNotNull(blackbox, "blackbox");

        GcStats stats = new GcStats();
        blackbox.scheduleAtFixedRate(stats, PUBLISH_FREQUENCY_SECONDS, SECONDS);
        blackbox.scanAndRegister(stats, "gc");
    }

    static class GcStats implements Runnable {
        @SensorInput
        volatile long minorCount;
        @SensorInput
        volatile long minorTime;
        @SensorInput
        volatile long majorCount;
        @SensorInput
        volatile long majorTime;
        @SensorInput
        volatile long unknownCount;
        @SensorInput
        volatile long unknownTime;

        @Override
        public void run() {
            long minorCount = 0;
            long minorTime = 0;
            long majorCount = 0;
            long majorTime = 0;
            long unknownCount = 0;
            long unknownTime = 0;

            for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                long count = gc.getCollectionCount();
                if (count == -1) {
                    continue;
                }

                if (YOUNG_GC.contains(gc.getName())) {
                    minorCount += count;
                    minorTime += gc.getCollectionTime();
                } else if (OLD_GC.contains(gc.getName())) {
                    majorCount += count;
                    majorTime += gc.getCollectionTime();
                } else {
                    unknownCount += count;
                    unknownTime += gc.getCollectionTime();
                }
            }

            this.minorCount = minorCount;
            this.minorTime = minorTime;
            this.majorCount = majorCount;
            this.majorTime = majorTime;
            this.unknownCount = unknownCount;
            this.unknownTime = unknownTime;
        }
    }
}
