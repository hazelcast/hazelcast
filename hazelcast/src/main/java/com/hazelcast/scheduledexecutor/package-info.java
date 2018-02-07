/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

/**
 * <p>This package contains ScheduledExecutorService functionality for Hazelcast.
 *
 * <p>The ScheduledExecutorService provides functionality similar to {@link java.util.concurrent.ExecutorService}
 * and also additional methods like executing tasks on a member who is owner of a specific key.
 * ScheduledExecutorService also provides a way to find the {@link com.hazelcast.scheduledexecutor.IScheduledFuture}
 * at any point in time using the {@link com.hazelcast.scheduledexecutor.ScheduledTaskHandler} accessible through
 * {@link com.hazelcast.scheduledexecutor.IScheduledFuture#getHandler()}
 *
 * @since 3.8
 */
package com.hazelcast.scheduledexecutor;
