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

import static com.hazelcast.internal.monitors.PerformanceMonitor.HUMAN_FRIENDLY_FORMAT;

abstract class PerformanceLog {

    protected final PerformanceMonitor monitor;
    protected final PerformanceLogWriter logWriter;

    PerformanceLog(PerformanceMonitor monitor) {
        this.monitor = monitor;
        this.logWriter = monitor.properties.getBoolean(HUMAN_FRIENDLY_FORMAT)
                ? new MultiLinePerformanceLogWriter()
                : new SingleLinePerformanceLogWriter() ;
    }


    abstract void render(PerformanceMonitorPlugin plugin);

     void close() {
    }

    public abstract void addStaticPlugin(PerformanceMonitorPlugin plugin);
}
