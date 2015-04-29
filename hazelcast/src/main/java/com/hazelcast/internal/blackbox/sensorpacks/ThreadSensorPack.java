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
import com.hazelcast.internal.blackbox.LongSensorInput;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A Sensor pack for exposing {@link ThreadMXBean} sensors.
 */
public final class ThreadSensorPack {

    private ThreadSensorPack() {
    }

    /**
     * Registers all the sensors in this sensor pack.
     *
     * @param blackBox the Blackbox the sensors are registered on.
     */
    public static void register(Blackbox blackBox) {
        checkNotNull(blackBox, "blackBox");

        ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

        blackBox.register(mxBean, "thread.threadCount", new LongSensorInput<ThreadMXBean>() {
                    @Override
                    public long get(ThreadMXBean threadMXBean) {
                        return threadMXBean.getThreadCount();
                    }
                }
        );

        blackBox.register(mxBean, "thread.peakThreadCount", new LongSensorInput<ThreadMXBean>() {
                    @Override
                    public long get(ThreadMXBean threadMXBean) {
                        return threadMXBean.getPeakThreadCount();
                    }
                }
        );

        blackBox.register(mxBean, "thread.daemonThreadCount", new LongSensorInput<ThreadMXBean>() {
                    @Override
                    public long get(ThreadMXBean threadMXBean) {
                        return threadMXBean.getDaemonThreadCount();
                    }
                }
        );

        blackBox.register(mxBean, "thread.totalStartedThreadCount", new LongSensorInput<ThreadMXBean>() {
                    @Override
                    public long get(ThreadMXBean threadMXBean) {
                        return threadMXBean.getTotalStartedThreadCount();
                    }
                }
        );
    }
}
