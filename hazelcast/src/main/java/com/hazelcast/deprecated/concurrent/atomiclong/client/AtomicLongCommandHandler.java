/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.deprecated.concurrent.atomiclong.client;

import com.hazelcast.deprecated.client.ClientCommandHandler;
import com.hazelcast.concurrent.atomiclong.AtomicLongService;
import com.hazelcast.concurrent.atomiclong.proxy.AtomicLongProxy;

public abstract class AtomicLongCommandHandler extends ClientCommandHandler {

    AtomicLongService als;

    public AtomicLongCommandHandler(AtomicLongService als) {
        this.als = als;
    }

    protected final AtomicLongProxy getAtomicLongProxy(String name) {
        return als.createDistributedObject(name);
    }
}
