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

/**
 * Represents abstract task;
 *
 * Tasks can be:
 *
 * <ul>
 *     <li>Container execution task</li>
 *     <li>Network socket writer</li>
 *     <li>Network socket reader</li>
 * </ul>
 *
 *
 * General architecture of any tasks usage is:
 * <pre>
 *      Producer_1                  Consumer_1
 *      Producer_2                  Consumer_2
 *
 *
 *
 *      .....           -&gt;  Task -&gt;
 *
 *
 *
 *      Producer_n                  Consumer_K
 * </pre>
 *
 * Examples of producers and consumers:
 *
 * <pre>
 *     Network socketChannel,
 *     Files,
 *     Hazelcast structures,
 *     HDFS,
 *     Previous RingBuffer with data from the previous container;
 * </pre>
 */
public interface Task {
    /**
     * @param classLoader - set classLoader to be used as T
     */
    void setThreadContextClassLoaders(ClassLoader classLoader);

    /**
     * Init task, perform initialization actions before task being executed;
     * The strict rule is that this method will be executed synchronously on
     * all nodes in cluster before any real task's  execution;
     */
    void init();

    /***
     * Will be invoked immediately before task was submitted into the executor,
     * strictly from executor-thread;
     */
    void beforeProcessing();

    /**
     * Interrupts tasks execution;
     *
     * @param error - the reason of the interruption;
     */
    void interrupt(Throwable error);

    /**
     * Execute next iteration of task;
     *
     * @param payload - payLoad which holds execution useful activity;
     * @return - true - if task should be executed again, false if task should be removed from executor;
     * @throws Exception if any exception
     */
    boolean executeTask(Payload payload) throws Exception;

    /**
     * Performs finalization actions after execution;
     * Task can be inited and executed again;
     */
    void finalizeTask();

    /**
     * Destroy task;
     * Task can not be executed again after destroy;
     */
    void destroy();
}
