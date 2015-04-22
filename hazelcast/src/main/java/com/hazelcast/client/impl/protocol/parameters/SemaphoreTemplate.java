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

@GenerateParameters(id = TemplateConstants.SEMAPHORE_TEMPLATE_ID,
        name = "Semaphore", ns = "Hazelcast.Client.Protocol.Semaphore")
public interface SemaphoreTemplate {

    @EncodeMethod(id = 1)
    void init(String name, int permits);

    @EncodeMethod(id = 2)
    void acquire(String name, int permits);

    @EncodeMethod(id = 3)
    void availablePermits(String name);

    @EncodeMethod(id = 4)
    void drainPermits(String name);

    @EncodeMethod(id = 5)
    void reducePermits(String name, int reduction);

    @EncodeMethod(id = 6)
    void release(String name, int permits);

    @EncodeMethod(id = 7)
    void tryAcquire(String name, int permits, long timeout);

}

