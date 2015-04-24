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

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A Sensor pack for exposing {@link java.lang.management.ClassLoadingMXBean} sensors.
 */
public final class ClassLoadingSensorPack {

    private ClassLoadingSensorPack() {
    }

    /**
     * Registers all the sensors in this sensor pack.
     *
     * @param blackbox the Blackbox the sensors are registered on.
     */
    public static void register(Blackbox blackbox) {
        checkNotNull(blackbox, "blackBox");

        ClassLoadingMXBean mxBean = ManagementFactory.getClassLoadingMXBean();

        blackbox.register(mxBean, "classloading.loadedClassesCount",
                new LongSensorInput<ClassLoadingMXBean>() {
                    @Override
                    public long get(ClassLoadingMXBean classLoadingMXBean) {
                        return classLoadingMXBean.getLoadedClassCount();
                    }
                }
        );

        blackbox.register(mxBean, "classloading.totalLoadedClassesCount",
                new LongSensorInput<ClassLoadingMXBean>() {
                    @Override
                    public long get(ClassLoadingMXBean classLoadingMXBean) {
                        return classLoadingMXBean.getTotalLoadedClassCount();
                    }
                }
        );

        blackbox.register(mxBean, "classloading.unloadedClassCount",
                new LongSensorInput<ClassLoadingMXBean>() {
                    @Override
                    public long get(ClassLoadingMXBean classLoadingMXBean) {
                        return classLoadingMXBean.getUnloadedClassCount();
                    }
                }
        );
    }
}
