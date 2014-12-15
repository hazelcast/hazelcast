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

package com.hazelcast.core;

import java.util.Map;

/**
 * MultiExecutionCallback provides notification for when an execution is completed on each member
 * that a task is submitted to. After all executions are completed on all submitted members,
 * the {@link #onComplete(java.util.Map)} method is called with a map of all results.
 *
 * @see IExecutorService
 * @see ExecutionCallback
 */
public interface MultiExecutionCallback {

    /**
     * Called when an execution is completed on a member.
     *
     * @param member member that the task is submitted to.
     * @param value result of the execution
     */
    void onResponse(Member member, Object value);

    /**
     * Called after all executions are completed.
     *
     * @param values map of Member-Response pairs
     */
    void onComplete(Map<Member, Object> values);
}
