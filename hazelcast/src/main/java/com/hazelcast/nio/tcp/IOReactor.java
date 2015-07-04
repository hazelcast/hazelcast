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

package com.hazelcast.nio.tcp;

import java.nio.channels.Selector;

/**
 * The name selector is not correct. A selector is a structure you can listen to certain events like
 * data available to read, or space available to write. A selector doesn't have a thread and a selector
 * also doesn't handle the actual logic. But the IOSelector add all that.
 */
public interface IOReactor {

    // too: not wanted
    Selector getSelector();

    void addTask(Runnable runnable);

    /**
     * Adds a task to be executed by the IOSelector and wakes up the IOSelector so that it will
     * eventually pick up the task.
     *
     * @param runnable the task to add.
     */
    void addTaskAndWakeup(Runnable runnable);

    void start();

    void shutdown();

    //todo: sucks
    void handleSelectionKeyFailure(Throwable e);

    /**
     * @deprecated will be removed when metrics are integrated
     */
    long getReadEvents();

    /**
     * @deprecated will be removed when metrics are integrated
     */
    long getWriteEvents();
}
