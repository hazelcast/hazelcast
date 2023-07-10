/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine;

/**
 * Every {@link TaskQueue} has a task factory. So arbitrary objects can be placed on the
 * {@link TaskQueue}, but once they hit the {@link Eventloop} they need to be
 * converted to some form of {@link Runnable} (also check the {@link Task} object).
 */
public interface TaskFactory {

    /**
     * Initializes the TaskFactory with the given eventloop.
     *
     * @param eventloop the Eventloop this TaskFactory belongs to.
     */
    void init(Eventloop eventloop);

    /**
     * Converts the given object to a {@link Task}.
     *
     * @param cmd
     * @return
     */
    Task toTask(Object cmd);
}
