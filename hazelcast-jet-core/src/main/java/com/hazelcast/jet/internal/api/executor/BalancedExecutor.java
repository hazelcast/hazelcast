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

package com.hazelcast.jet.internal.api.executor;


/**
 * Represents abstract balanced executor;
 * Balancing means that tasks can be migrated from
 * one thread to another to provide load-balancing between threads;
 */
public interface BalancedExecutor extends AbstractExecutor {
    /**
     * Set flag to executor - that it was balanced;
     */
    void setBalanced();

    /**
     * @return - true if executor is balanced, false -otherwise;
     */
    boolean isBalanced();

    /**
     * Register processor which will be used to re-balance tasks among another threads;
     *
     * @param taskProcessor - corresponding processor
     * @return - true - processor has been successfully registered, false - otherwise;
     */
    boolean registerUnBalanced(BalancedWorkingProcessor taskProcessor);

    /**
     * Will be invoked before task will be deleted from executor;
     */
    void onTaskDeactivation();

    /**
     * @return - number of task in executor;
     */
    int getTaskCount();
}
