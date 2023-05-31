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
 * A {@link TaskGroup} can have a processor. Any 'task' that is offered to a TaskGroup
 * that isn't a runnable, will be offered to the processor. The processor should try to
 * process that task.
 */
public interface Processor {

    int PROCESS_STATUS_COMPLETED = 0;
    int PROCESS_STATUS_BLOCKED = 1;

    /**
     * Initializes the scheduler with the given eventloop.
     *
     * @param eventloop the Eventloop.
     */
    void init(Eventloop eventloop);

    /**
     * Gives the scheduler a tick. In this tick the scheduler can do a bit of work.
     *
     * @return true if there is more work, false otherwise.
     */
    int process(Object cmd);
}
