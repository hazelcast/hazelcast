/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.tcp;

import com.hazelcast.logging.ILogger;

import java.nio.channels.Selector;

/**
 * The IOReactor is an implementation of the reactor design-pattern.
 * <p/>
 * http://www.cs.wustl.edu/~schmidt/PDF/reactor-siemens.pdf
 * <p/>
 * The reactor contains a few parts:
 * - the reactor itself.
 * - the event loop
 * - the event handler
 */
public interface IOReactor {

    Selector getSelector();

    ILogger getLogger();

    IOReactorOutOfMemoryHandler getOutOfMemoryHandler();

    String getName();


    void addTask(Runnable runnable);

    /**
     * Processes all tasks.
     *
     * It is important that the implementation processes all tasks. If a task schedules itself again, also this task is going
     * to be processed.
     */
    void processTasks();

    void wakeup();

    void start();

    void shutdown();

    void awaitShutdown();

    boolean isAlive();

    void dumpPerformanceMetrics(StringBuffer sb);
}
