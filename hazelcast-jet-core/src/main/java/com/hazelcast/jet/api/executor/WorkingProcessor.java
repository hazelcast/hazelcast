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

package com.hazelcast.jet.api.executor;

import java.util.concurrent.Future;

/**
 * Represents abstract working processor for execution in thread-pool;
 */
public interface WorkingProcessor extends Runnable, TaskConsumer {
    /**
     * Start processor's execution;
     */
    void start();

    /**
     * Notify processor to wakeUp;
     */
    void wakeUp();

    /**
     * @return - number of tasks inside container;
     */
    int getWorkingTaskCount();

    /**
     * Asynchronously shutdown processor;
     *
     * @return - awaiting Future object;
     */
    Future<Boolean> shutdown();
}
