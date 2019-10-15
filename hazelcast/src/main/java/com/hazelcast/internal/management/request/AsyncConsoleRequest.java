/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management.request;

/**
 * Represents async requests sent from Management Center.
 * <p>
 * Normally, ManagementCenterService handles {@code ConsoleRequest}s synchronously
 * on its {@code TaskPollThread}, a single thread polls tasks from Management Center,
 * executes them and writes back the response directly. This is fine for short running tasks
 * but for long running tasks or blocking invocations, it's required to offload request
 * handling to an async thread pool. Otherwise, {@code TaskPollThread} will be blocked
 * for a long time without handling any request from Management Center.
 *
 * @see ConsoleRequest
 * @since 3.6
 */
public interface AsyncConsoleRequest extends ConsoleRequest {
}
