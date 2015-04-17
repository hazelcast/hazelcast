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
import com.hazelcast.nio.serialization.Data;

@GenerateParameters(id = 6, name = "AtomicLong", ns = "Hazelcast.Client.Protocol.AtomicLong")
public interface AtomicLongTemplate {

    @EncodeMethod(id = 1)
    void apply(String name, Data function);

    @EncodeMethod(id = 2)
    void alter(String name, Data function);

    @EncodeMethod(id = 3)
    void alterAndGet(String name, Data function);

    @EncodeMethod(id = 4)
    void getAndAlter(String name, Data function);

    @EncodeMethod(id = 5)
    void addAndGet(String name, long delta);

    @EncodeMethod(id = 6)
    void compareAndSet(String name, long expected, long updated);

    @EncodeMethod(id = 7)
    void decrementAndGet(String name);

    @EncodeMethod(id = 8)
    void get(String name);

    @EncodeMethod(id = 9)
    void getAndAdd(String name, long delta);

    @EncodeMethod(id = 10)
    void getAndSet(String name, long newValue);

    @EncodeMethod(id = 11)
    void incrementAndGet(String name);

    @EncodeMethod(id = 12)
    void getAndIncrement(String name);

    @EncodeMethod(id = 13)
    void set(String name, long newValue);

}
