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

/**
 * <p>This package contains IExecutorService functionality for Hazelcast.<br>
 * The IExecutorService extends the {@link java.util.concurrent.ExecutorService} and provides all kinds
 * of additional methods related to distributed systems. You can execute a task on a particular member, all members
 * or a subset of members. You can also execute a task on a member which own a particular partition. This is very
 * useful if you want to reduce the number of remote calls; it is better to send the logic to the data than to
 * pull the data to the logic.
 *
 * @since 1
 */
package com.hazelcast.executor.impl;
