/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.waitnotifyservice;

import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitNotifyService;
import com.hazelcast.spi.impl.OperationTracingService;

public interface InternalWaitNotifyService extends WaitNotifyService, OperationTracingService {

    void cancelWaitingOps(String serviceName, Object objectId, Throwable cause);

    /**
     * Interrupts an operation.
     *
     * If the operation doesn't exist, has completed, or already is interrupted, the call is ignored.
     *
     * @param key the WaitNotifyKey if this Operation
     * @param callerUUID the id of the calling member
     * @param callId the callId.
     */
    void interrupt(WaitNotifyKey key, String callerUUID, long callId);
}
