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

/**
 * ExecutionCallback allows to asynchronously get notified when the execution is completed,
 * either successfully or with error.
 *
 * @see IExecutorService#submit(java.util.concurrent.Callable, ExecutionCallback)
 * @see IExecutorService#submitToMember(java.util.concurrent.Callable, Member, ExecutionCallback)
 * @see IExecutorService#submitToKeyOwner(java.util.concurrent.Callable, Object, ExecutionCallback)
 *
 * @param <V> value
 */
public interface ExecutionCallback<V> {

    /**
     * Called when an execution is completed successfully.
     *
     * @param response result of execution
     */
    void onResponse(V response);

    /**
     * Called when an execution is completed with an error.
     * @param t exception thrown
     */
    void onFailure(Throwable t);
}
