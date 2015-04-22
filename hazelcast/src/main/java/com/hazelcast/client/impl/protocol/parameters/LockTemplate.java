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

package com.hazelcast.client.impl.protocol.parameters;

import com.hazelcast.annotation.EncodeMethod;
import com.hazelcast.annotation.GenerateParameters;

@GenerateParameters(id = 10, name = "Lock", ns = "Hazelcast.Client.Protocol.Lock")
public interface LockTemplate {

    @EncodeMethod(id = 1)
    void isLocked(String name, long threadId);

    @EncodeMethod(id = 2)
    void isLockedByCurrentThread(String name, long threadId);

    @EncodeMethod(id = 3)
    void getLockCount(String name);

    @EncodeMethod(id = 4)
    void getRemainingLeaseTime(String name);

    @EncodeMethod(id = 5)
    void lock(String name, long leaseTime, long threadId);

    @EncodeMethod(id = 6)
    void unlock(String name, long threadId);

    @EncodeMethod(id = 7)
    void forceUnlock(String name);

    @EncodeMethod(id = 8)
    void tryLock(String name, long threadId, long timeout);

}
