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

package com.hazelcast.client.impl.protocol.template;

import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Request;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;

@GenerateCodec(id = TemplateConstants.CONDITION_TEMPLATE_ID,
        name = "Condition", ns = "Hazelcast.Client.Protocol.Codec")
public interface ConditionCodecTemplate {
    /**
     * Causes the current thread to wait until it is signalled or interrupted, or the specified waiting time elapses.
     *
     * @param name     Name of the Condition
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param timeout  The maximum time to wait
     * @param lockName Name of the lock to wait on.
     * @return False if the waiting time detectably elapsed before return from the method, else true
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "lockName")
    Object await(String name, long threadId, long timeout, String lockName);

    /**
     * Causes the current thread to wait until it is signalled or Thread#interrupt interrupted. The lock associated with
     * this Condition is atomically released and the current thread becomes disabled for thread scheduling purposes and
     * lies dormant until one of four things happens:
     * Some other thread invokes the #signal method for this Condition and the current thread happens to be chosen as the
     * thread to be awakened; or Some other thread invokes the #signalAll method for this Condition; or Some other thread
     * Thread#interrupt interrupts the current thread, and interruption of thread suspension is supported; or A spurious wakeup occurs.
     * In all cases, before this method can return the current thread must re-acquire the lock associated with this condition.
     * When the thread returns it is guaranteed to hold this lock. If the current thread: has its interrupted status set
     * on entry to this method; or is Thread#interrupt interrupted while waiting and interruption of thread suspension
     * is supported, then INTERRUPTED is thrown and the current thread's interrupted status is cleared. It is
     * not specified, in the first case, whether or not the test for interruption occurs before the lock is released.
     * The current thread is assumed to hold the lock associated with this Condition when this method is called.
     * It is up to the implementation to determine if this is the case and if not, how to respond. Typically, an exception will be
     * thrown (such as ILLEGAL_MONITOR_STATE) and the implementation must document that fact.
     * An implementation can favor responding to an interrupt over normal method return in response to a signal. In that
     * case the implementation must ensure that the signal is redirected to another waiting thread, if there is one.
     *
     * @param name     Name of the Condition
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param lockName Name of the lock to wait on.
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "lockName")
    void beforeAwait(String name, long threadId, String lockName);

    /**
     * If any threads are waiting on this condition then one is selected for waking up.That thread must then re-acquire
     * the lock before returning from await. An implementation may (and typically does) require that the
     * current thread hold the lock associated with this Condition when this method is called. Implementations must
     * document this precondition and any actions taken if the lock is not held. Typically, an exception such as
     * ILLEGAL_MONITOR_STATE will be thrown.
     *
     * @param name     Name of the Condition
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param lockName Name of the lock to wait on.
     */

    @Request(id = 3, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "lockName")
    void signal(String name, long threadId, String lockName);

    /**
     * If any threads are waiting on this condition then they are all woken up. Each thread must re-acquire the lock
     * before it can return from
     *
     * @param name     Name of the Condition
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param lockName Name of the lock to wait on.
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "lockName")
    void signalAll(String name, long threadId, String lockName);

}
