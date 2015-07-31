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

    @Request(id = 1, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void isLocked(String name);

    @Request(id = 2, retryable = true, response = ResponseMessageConst.BOOLEAN)
    void isLockedByCurrentThread(String name, long threadId);

    @Request(id = 3, retryable = true, response = ResponseMessageConst.INTEGER)
    void getLockCount(String name);

    @Request(id = 4, retryable = true, response = ResponseMessageConst.LONG)
    void getRemainingLeaseTime(String name);

    @Request(id = 5, retryable = false, response = ResponseMessageConst.VOID)
    void lock(String name, long leaseTime, long threadId);

    @Request(id = 6, retryable = false, response = ResponseMessageConst.VOID)
    void unlock(String name, long threadId);

    @Request(id = 7, retryable = false, response = ResponseMessageConst.VOID)
    void forceUnlock(String name);

    @Request(id = 8, retryable = false, response = ResponseMessageConst.BOOLEAN)
    void tryLock(String name, long threadId, long timeout);
}
