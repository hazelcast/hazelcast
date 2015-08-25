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

@GenerateCodec(id = TemplateConstants.LOCK_TEMPLATE_ID, name = "Lock", ns = "Hazelcast.Client.Protocol.Lock")
public interface LockCodecTemplate {

    /**
     *
     * @param name Name of the Lock
     * @return True if this lock is locked, false otherwise.
     */
    @Request(id = 1, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object isLocked(String name);

    /**
     *
     * @param name Name of the Lock
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return True if this lock is locked by current thread, false otherwise.
     */
    @Request(id = 2, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object isLockedByCurrentThread(String name, long threadId);

    /**
     *
     * @param name Name of the Lock
     * @return The lock hold count.
     */
    @Request(id = 3, retryable = true, response = ResponseMessageConst.INTEGER)
    Object getLockCount(String name);

    /**
     *
     * @param name Name of the Lock
     * @return Remaining lease time in milliseconds.
     */
    @Request(id = 4, retryable = true, response = ResponseMessageConst.LONG)
    Object getRemainingLeaseTime(String name);

    /**
     *
     * @param name Name of the Lock
     * @param leaseTime Time to wait before releasing to lock
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.VOID)
    void lock(String name, long leaseTime, long threadId);

    /**
     *
     * @param name Name of the Lock
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.VOID)
    void unlock(String name, long threadId);

    /**
     *
     * @param name Name of the Lock
     */
    @Request(id = 7, retryable = false, response = ResponseMessageConst.VOID)
    void forceUnlock(String name);

    /**
     *
     * @param name Name of the Lock
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param lease time in milliseconds to wait before releasing the lock.
     * @param timeout Maximum time to wait for the lock.
     * @return true if the lock was acquired and false if the waiting time elapsed before the lock was acquired.
     */
    @Request(id = 8, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object tryLock(String name, long threadId, long lease, long timeout);

}
