/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

/**
 * A service for object where one can wait on, like ICondition or a ILock when a thread needs
 * to wait for a lock to be released.
 */
public interface WaitNotifyService {

    /**
     * Causes the current operation to wait in WaitNotifyService until it is notified
     * by a {@link com.hazelcast.spi.Notifier} operation or timeout specified by
     * {@link WaitSupport#getWaitTimeout()} passes.
     * <p/>
     * {@link com.hazelcast.spi.WaitSupport} operation will be registered using {@link com.hazelcast.spi.WaitNotifyKey}
     * returned from method {@link WaitSupport#getWaitKey()}.
     * <p/>
     * When operation is notified, it's re-executed by related scheduled mechanism.
     * <p/>
     * If wait time-outs, {@link WaitSupport#onWaitExpire()} method is called.
     * <p/>
     * This method should be called in the thread executes the actual {@link com.hazelcast.spi.WaitSupport} operation.
     *
     * @param waitSupport operation which will wait for notification
     */
    void await(WaitSupport waitSupport);

    /**
     * Notifies the waiting {@link com.hazelcast.spi.WaitSupport} operation to wake-up and continue executing.
     * <p/>
     * A waiting operation registered with the {@link Notifier#getNotifiedKey()} will be notified and deregistered.
     * This method has no effect if there isn't any operation registered
     * for related {@link com.hazelcast.spi.WaitNotifyKey}.
     * <p/>
     * This method should be called in the thread executes the actual {@link com.hazelcast.spi.Notifier} operation.
     *
     * @param notifier operation which will notify a corresponding waiting operation
     */
    void notify(Notifier notifier);

}
