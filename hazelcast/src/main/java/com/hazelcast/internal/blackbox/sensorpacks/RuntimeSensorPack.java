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
import java.lang.management.RuntimeMXBean;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A Sensor pack for exposing {@link java.lang.Runtime} sensors.
 */
public final class RuntimeSensorPack {

    private RuntimeSensorPack() {
    }

    /**
     * Registers all the sensors in this sensor pack.
     *
     * @param blackbox the Blackbox the sensors are registered on.
     */
    public static void register(Blackbox blackbox) {
        checkNotNull(blackbox, "blackbox");

        Runtime runtime = Runtime.getRuntime();
        RuntimeMXBean mxBean = ManagementFactory.getRuntimeMXBean();

        blackbox.register(runtime, "runtime.freeMemory", new LongSensorInput<Runtime>() {
                    @Override
                    public long get(Runtime runtime) {
                        return runtime.freeMemory();
                    }
                }
        );

        blackbox.register(runtime, "runtime.totalMemory", new LongSensorInput<Runtime>() {
                    @Override
                    public long get(Runtime runtime) {
                        return runtime.totalMemory();
                    }
                }
        );

        blackbox.register(runtime, "runtime.maxMemory", new LongSensorInput<Runtime>() {
                    @Override
                    public long get(Runtime runtime) {
                        return runtime.maxMemory();
                    }
                }
        );

        blackbox.register(runtime, "runtime.usedMemory", new LongSensorInput<Runtime>() {
                    @Override
                    public long get(Runtime runtime) {
                        return runtime.totalMemory() - runtime.freeMemory();
                    }
                }
        );

        blackbox.register(runtime, "runtime.availableProcessors", new LongSensorInput<Runtime>() {
                    @Override
                    public long get(Runtime runtime) {
                        return runtime.availableProcessors();
                    }
                }
        );

        blackbox.register(mxBean, "runtime.uptime", new LongSensorInput<RuntimeMXBean>() {
                    @Override
                    public long get(RuntimeMXBean runtimeMXBean) {
                        return runtimeMXBean.getUptime();
                    }
                }
        );
    }
}
