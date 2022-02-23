/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cp.internal.datastructures.countdownlatch;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchAwaitCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchCountDownCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchGetCountCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchGetRoundCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchTrySetCountCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import com.hazelcast.internal.util.EmptyStatement;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;

/**
 * Client-side Raft-based proxy implementation of {@link ICountDownLatch}
 */
public class CountDownLatchProxy extends ClientProxy implements ICountDownLatch {

    private final RaftGroupId groupId;
    private final String objectName;

    public CountDownLatchProxy(ClientContext context, RaftGroupId groupId, String proxyName, String objectName) {
        super(CountDownLatchService.SERVICE_NAME, proxyName, context);
        this.groupId = groupId;
        this.objectName = objectName;
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) {
        checkNotNull(unit);

        long timeoutMillis = Math.max(0, unit.toMillis(timeout));
        ClientMessage request = CountDownLatchAwaitCodec.encodeRequest(groupId, objectName, newUnsecureUUID(), timeoutMillis);
        ClientMessage response = new ClientInvocation(getClient(), request, name).invoke().joinInternal();

        return CountDownLatchAwaitCodec.decodeResponse(response);
    }

    @Override
    public void countDown() {
        int round = getRound();
        UUID invocationUid = newUnsecureUUID();
        for (;;) {
            try {
                countDown(round, invocationUid);
                return;
            } catch (OperationTimeoutException e) {
                EmptyStatement.ignore(e);
                // I can retry safely because my retry would be idempotent...
            }
        }
    }

    private int getRound() {
        ClientMessage request = CountDownLatchGetRoundCodec.encodeRequest(groupId, objectName);
        ClientMessage response = new ClientInvocation(getClient(), request, name).invoke().joinInternal();

        return CountDownLatchGetRoundCodec.decodeResponse(response);
    }

    private void countDown(int round, UUID invocationUid) {
        ClientMessage request = CountDownLatchCountDownCodec.encodeRequest(groupId, objectName, invocationUid, round);

        new ClientInvocation(getClient(), request, name).invoke().joinInternal();
    }

    @Override
    public int getCount() {
        ClientMessage request = CountDownLatchGetCountCodec.encodeRequest(groupId, objectName);
        ClientMessage response = new ClientInvocation(getClient(), request, name).invoke().joinInternal();

        return CountDownLatchGetCountCodec.decodeResponse(response);
    }

    @Override
    public boolean trySetCount(int count) {
        checkPositive(count, "Count must be positive!");

        ClientMessage request = CountDownLatchTrySetCountCodec.encodeRequest(groupId, objectName, count);
        ClientMessage response = new ClientInvocation(getClient(), request, name).invoke().joinInternal();

        return CountDownLatchTrySetCountCodec.decodeResponse(response);
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    public CPGroupId getGroupId() {
        return groupId;
    }

    @Override
    public void onDestroy() {
        ClientMessage request = CPGroupDestroyCPObjectCodec.encodeRequest(groupId, getServiceName(), objectName);
        new ClientInvocation(getClient(), request, name).invoke().joinInternal();
    }

}
