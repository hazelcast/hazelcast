/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.spi;

import com.hazelcast.spi.impl.executionservice.TaskScheduler;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * Executor service for Hazelcast clients.
 * <p>
 * Allows asynchronous execution and scheduling of {@link Runnable} and {@link Callable} commands.
 * <p>
 * Any schedule submit or execute operation runs on internal executors.
 * When user code needs to run getUserExecutor() should be utilized
 */
public interface ClientExecutionService extends TaskScheduler {

    /**
     * @return executorService that alien (user code) runs on
     */
    ExecutorService getUserExecutor();

}
