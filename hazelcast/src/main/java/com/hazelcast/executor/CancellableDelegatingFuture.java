/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.executor;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.DelegatingFuture;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;

/**
 * @mdogan 6/7/13
 */
final class CancellableDelegatingFuture<V> extends DelegatingFuture<V> {

    private final NodeEngine nodeEngine;
    private final String uuid;
    private final int partitionId;
    private final Address target;
    private volatile boolean cancelled;

    CancellableDelegatingFuture(Future future, NodeEngine nodeEngine, String uuid, int partitionId) {
        super(future, nodeEngine.getSerializationService());
        this.nodeEngine = nodeEngine;
        this.uuid = uuid;
        this.partitionId = partitionId;
        this.target = null;
    }

    CancellableDelegatingFuture(Future future, NodeEngine nodeEngine, String uuid, Address target) {
        super(future, nodeEngine.getSerializationService());
        this.nodeEngine = nodeEngine;
        this.uuid = uuid;
        this.target = target;
        this.partitionId = -1;
    }

    CancellableDelegatingFuture(Future future, V defaultValue, NodeEngine nodeEngine, String uuid, int partitionId) {
        super(future, nodeEngine.getSerializationService(), defaultValue);
        this.nodeEngine = nodeEngine;
        this.uuid = uuid;
        this.partitionId = partitionId;
        this.target = null;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (isDone() || cancelled) {
            return false;
        }
        final CancellationOperation op = new CancellationOperation(uuid, mayInterruptIfRunning);
        final InvocationBuilder builder;
        if (partitionId > -1) {
            builder = nodeEngine.getOperationService().createInvocationBuilder(DistributedExecutorService.SERVICE_NAME,
                    op, partitionId);
        } else {
            builder = nodeEngine.getOperationService().createInvocationBuilder(DistributedExecutorService.SERVICE_NAME,
                    op, target);
        }
        builder.setTryCount(50).setTryPauseMillis(250);

        final Invocation inv = builder.build();
        final Future f = inv.invoke();
        try {
            final Boolean b = (Boolean) f.get();
            if (b != null && b) {
                setError(new CancellationException());
                cancelled = true;
                return true;
            }
            return false;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        } finally {
            setDone();
        }
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }
}
