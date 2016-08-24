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

package com.hazelcast.jet.impl.executor;

import com.hazelcast.jet.impl.util.BooleanHolder;

/**
 * Represents abstract task
 * <p/>
 * Tasks can be:
 * <p/>
 * <ul>
 * <li>Container execution task</li>
 * <li>Network socket writer</li>
 * <li>Network socket reader</li>
 * </ul>
 * <p/>
 * <p/>
 * General architecture of any tasks usage is:
 * <pre>
 *      Producer_1                  Consumer_1
 *      Producer_2                  Consumer_2
 *
 *
 *
 *      .....           -&gt  Task -&gt
 *
 *
 *
 *      Producer_n                  Consumer_K
 * </pre>
 * <p/>
 * Examples of producers and consumers:
 * <p/>
 * <pre>
 *     Network socketChannel,
 *     Files,
 *     Hazelcast structures,
 *     HDFS,
 *     Previous RingBuffer with data from the previous container
 * </pre>
 */
public abstract class Task {

    protected volatile ClassLoader contextClassLoader;

    /**
     * @param classLoader - set classLoader to be used as T
     */
    public void setThreadContextClassLoader(ClassLoader classLoader) {
        this.contextClassLoader = classLoader;
    }

    /**
     * Init task, perform initialization actions before task being executed
     * The strict rule is that this method will be executed synchronously on
     * all nodes in cluster before any real task's  execution
     */
    public void init() {

    }

    /**
     * Destroy task
     * Task can not be executed again after destroy
     */
    public void destroy() {

    }

    /**
     * Interrupts tasks execution
     *
     * @param error - the reason of the interruption
     */
    public void interrupt(Throwable error) {

    }

    /***
     * Will be invoked immediately before task was submitted into the executor,
     * strictly from executor-thread
     */
    public void beforeProcessing() {

    }

    /**
     * Performs finalization actions after execution
     * Task can be inited and executed again
     */
    public void finalizeTask() {

    }

    /**
     * Execute next iteration of task
     *
     * @param didWorkHolder flag to set to indicate that the task did something useful
     * @return - true - if task should be executed again, false if task should be removed from executor
     * @throws Exception if any exception
     */
    public abstract boolean execute(BooleanHolder didWorkHolder) throws Exception;
}
