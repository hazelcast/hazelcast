/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.impl.operationparker.OperationParker;

/**
 * An interface that can be implemented by an operation that can block. For example an IQueue.take().
 */
public interface BlockingOperation {

    WaitNotifyKey getWaitKey();

    /**
     * @implNote There can be multiple operations waiting in a queue for given {@link WaitNotifyKey}.
     * If this method returns {@code true}, no other {@link BlockingOperation} waiting for the same key will be unparked by given
     * {@link OperationParker#unpark(Notifier)} invocation. This usually is fine for locks,
     * but if the waiting operations are independent, it may be desirable to notify them all. In such case this method should
     * return {@code false} and the operation can be parked by returning {@link CallStatus#WAIT} from {@link Operation#call()}.
     * Note that if the operation blocks in {@link Operation#call()} again
     * it will be added at the end of the queue.
     */
    boolean shouldWait();

    long getWaitTimeout();

    void onWaitExpire();

    /**
     * Unparks this operation by submitting it to the specified {@link OperationService}.
     * <p>
     * This is used by parked operations to resume processing once the unpark condition
     * is met, or to reschedule the operation for execution after the current execution
     * cycle.
     * @implNote
     * By default, the {@code OperationService} executes the operation
     * immediately and synchronously in the calling thread. Implementations are free
     * to change this behavior, for example by queuing the operation to be executed later.
     * @param operationService the Hazelcast operation service used to run this operation
     */
    default void unpark(OperationService operationService) {
        operationService.run((Operation) this);
    }
}
