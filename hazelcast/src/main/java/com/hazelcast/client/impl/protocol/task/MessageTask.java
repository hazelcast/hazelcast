/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task;

/**
 * Interface for all client message tasks to implement
 */
public interface MessageTask extends Runnable {

    /**
     * Returns {@code true} if current task is a management one and should be protected by additional access checks (e.g. the
     * client source address).
     *
     * @return {@code true} when the task is a protected management task; {@code false} otherwise
     */
    boolean isManagementTask();
}
