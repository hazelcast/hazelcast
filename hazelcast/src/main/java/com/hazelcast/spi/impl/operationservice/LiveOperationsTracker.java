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

package com.hazelcast.spi.impl.operationservice;

/**
 * Can be implemented by a Service to track live operations. This functionality is needed to let the executing side
 * inform the caller side which operations are still running. If an operation doesn't provide a heartbeat, the caller
 * can eventually decide to timeout the operation.
 *
 * Some operations are not executing on regular operation threads (e.g. IExecutorService) or not running at all
 * (blocking operations).
 */
public interface LiveOperationsTracker {

    /**
     * Populate the LiveOperations
     *
     * @param liveOperations the LiveOperations to populate.
     */
    void populate(LiveOperations liveOperations);
}
