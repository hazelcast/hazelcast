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

package com.hazelcast.cp.internal.datastructures.countdownlatch.proxy;

import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.RaftCountDownLatchService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.operation.AwaitOp;
import com.hazelcast.cp.internal.datastructures.countdownlatch.operation.CountDownOp;
import com.hazelcast.cp.internal.datastructures.countdownlatch.operation.GetCountOp;
import com.hazelcast.cp.internal.datastructures.countdownlatch.operation.GetRoundOp;
import com.hazelcast.cp.internal.datastructures.countdownlatch.operation.TrySetCountOp;
import com.hazelcast.cp.internal.datastructures.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ProxyService;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;

/**
 * Server-side Raft-based proxy implementation of {@link ICountDownLatch}
 */
public class RaftCountDownLatchProxy implements ICountDownLatch {

    private final RaftInvocationManager invocationManager;
    private final ProxyService proxyService;
    private final RaftGroupId groupId;
    private final String proxyName;
    private final String objectName;

    public RaftCountDownLatchProxy(NodeEngine nodeEngine, RaftGroupId groupId, String proxyName, String objectName) {
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        this.invocationManager = service.getInvocationManager();
        this.proxyService = nodeEngine.getProxyService();
        this.groupId = groupId;
        this.proxyName = proxyName;
        this.objectName = objectName;
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) {
        checkNotNull(unit);

        long timeoutMillis = Math.max(0, unit.toMillis(timeout));
        return invocationManager.<Boolean>invoke(groupId, new AwaitOp(objectName, newUnsecureUUID(), timeoutMillis)).join();
    }

    @Override
    public void countDown() {
        int round = invocationManager.<Integer>invoke(groupId, new GetRoundOp(objectName)).join();
        invocationManager.invoke(groupId, new CountDownOp(objectName, newUnsecureUUID(), round)).join();
    }

    @Override
    public int getCount() {
        return invocationManager.<Integer>invoke(groupId, new GetCountOp(objectName)).join();
    }

    @Override
    public boolean trySetCount(int count) {
        return invocationManager.<Boolean>invoke(groupId, new TrySetCountOp(objectName, count)).join();
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return proxyName;
    }

    @Override
    public String getServiceName() {
        return RaftCountDownLatchService.SERVICE_NAME;
    }

    @Override
    public void destroy() {
        invocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), objectName)).join();
        proxyService.destroyDistributedObject(getServiceName(), proxyName);
    }

    public CPGroupId getGroupId() {
        return groupId;
    }

}
