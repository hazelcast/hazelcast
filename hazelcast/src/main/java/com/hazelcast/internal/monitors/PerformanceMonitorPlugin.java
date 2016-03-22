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

package com.hazelcast.internal.monitors;

/**
 * A Plugin for the {@link PerformanceMonitor}.
 *
 * The plugin will not be called concurrently, unless threads are introduced outside of the PerformanceMonitor.
 *
 * There is a happens before relation between {@link #onStart()} and {@link #run(PerformanceLogWriter)}, and therefor
 * there is no need to make variables volatile. The source of the happens-before relation is the scheduler.queue inside
 * of the PerformanceMonitor or the AtomicReference in case of static plugins.
 */
public abstract class PerformanceMonitorPlugin {

    /**
     * Indicates that a plugin should be run once per performance log file.
     */
    static final long STATIC = -1;

    /**
     * Indicates that the plugin is disabled.
     */
    static final long DISABLED = 0;

    /**
     * Returns the period of executing the monitor in millis.
     *
     * If a monitor is disabled, 0 is returned.
     *
     * If a monitor should run only once, a negative value is returned. This is useful for 'static' monitors like the
     * {@link SystemPropertiesPlugin} that run at the beginning of a log file but their contents will not change.
     *
     * @return the period in millis.
     */
    public abstract long getPeriodMillis();

    public abstract void onStart();

    public abstract void run(PerformanceLogWriter writer);
}
