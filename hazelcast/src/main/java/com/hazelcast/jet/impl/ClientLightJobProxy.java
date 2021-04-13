/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.jet.LightJob;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class ClientLightJobProxy implements LightJob {

    private final ClientInvocationFuture future;

    ClientLightJobProxy(ClientInvocationFuture future) {
        this.future = future;
    }

    @Override
    public void join() {
        try {
            future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public void cancel() {
        throw new UnsupportedOperationException("todo");
    }
}

