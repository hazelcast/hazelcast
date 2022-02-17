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

package com.hazelcast.spi.impl.operationparker;

import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

/**
 * A service to park/unpark {@link BlockingOperation}. E.g. to implement a {@link java.util.concurrent.locks.Lock#lock()} or
 * {@link java.util.concurrent.locks.Condition#await()}.
 */
public interface OperationParker {

    String SERVICE_NAME = "hz:impl:operationParker";

    /**
     * Parks the Operation until it is unparked by a {@link #unpark(Notifier)} call or by a timeout specified by the
     * {@link BlockingOperation#getWaitTimeout()}. After the {@link BlockingOperation} is parked, this method returns;
     * it doesn't wait.
     *
     * {@link BlockingOperation} operation will be registered using {@link WaitNotifyKey}
     * returned from method {@link BlockingOperation#getWaitKey()}.
     *
     * If wait time-outs, {@link BlockingOperation#onWaitExpire()} method is called.
     *
     * This method should be called in the thread that executes the actual {@link BlockingOperation} operation.
     *
     *
     * @param op operation which will wait for notification
     */
    void park(BlockingOperation op);

    /**
     * Unparks the parked {@link BlockingOperation} operation by rescheduling it on the
     * {@link com.hazelcast.spi.impl.operationexecutor.OperationExecutor}
     *
     * A parked operation registered with the {@link Notifier#getNotifiedKey()} will be notified and deregistered.
     * This method has no effect if there isn't any operation registered
     * for related {@link WaitNotifyKey}.
     *
     * This method should be called in the thread that executes the actual {@link Notifier} operation.
     *
     * @param notifier operation which will unpark a corresponding waiting operation
     */
    void unpark(Notifier notifier);

    void cancelParkedOperations(String serviceName, Object objectId, Throwable cause);
}
