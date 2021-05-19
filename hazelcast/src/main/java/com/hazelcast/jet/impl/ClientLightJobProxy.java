/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.jet.LightJob;
import com.hazelcast.jet.impl.client.protocol.codec.JetCancelLightJobCodec;
import com.hazelcast.jet.impl.util.NonCompletableFuture;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class ClientLightJobProxy implements LightJob {

    private final HazelcastClientInstanceImpl client;
    private final UUID coordinatorUuid;
    private final long jobId;
    private final NonCompletableFuture future;

    ClientLightJobProxy(HazelcastClientInstanceImpl client, UUID coordinatorUuid, long jobId, ClientInvocationFuture future) {
        this.client = client;
        this.coordinatorUuid = coordinatorUuid;
        this.jobId = jobId;
        this.future = new NonCompletableFuture(future);
    }

    @Override
    public long getId() {
        return jobId;
    }

    @Override @Nonnull
    public CompletableFuture<Void> getFuture() {
        return future;
    }

    @Override
    public void cancel() {
        ClientMessage message = JetCancelLightJobCodec.encodeRequest(jobId);
        ClientInvocation invocation = new ClientInvocation(client, message, null, coordinatorUuid);
        invocation.invoke().join();
    }
}

