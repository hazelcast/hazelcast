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

package com.hazelcast.cp.internal.datastructures.atomicref.proxy;

import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.core.IFunction;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefService;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.CompareAndSetOp;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.ContainsOp;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.GetOp;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.SetOp;
import com.hazelcast.cp.internal.datastructures.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.serialization.SerializationService;

import static com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp.ReturnValueType.NO_RETURN_VALUE;
import static com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp.ReturnValueType.RETURN_NEW_VALUE;
import static com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp.ReturnValueType.RETURN_OLD_VALUE;
import static com.hazelcast.cp.internal.raft.QueryPolicy.LINEARIZABLE;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * Server-side Raft-based proxy implementation of {@link IAtomicReference}
 *
 * @param <T> type of the referenced values
 */
public class AtomicRefProxy<T> implements IAtomicReference<T> {

    private final RaftInvocationManager invocationManager;
    private final SerializationService serializationService;
    private final RaftGroupId groupId;
    private final String proxyName;
    private final String objectName;

    public AtomicRefProxy(NodeEngine nodeEngine, RaftGroupId groupId, String proxyName, String objectName) {
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        this.invocationManager = service.getInvocationManager();
        this.serializationService = nodeEngine.getSerializationService();
        this.groupId = groupId;
        this.proxyName = proxyName;
        this.objectName = objectName;
    }

    @Override
    public boolean compareAndSet(T expect, T update) {
        return compareAndSetAsync(expect, update).joinInternal();
    }

    @Override
    public T get() {
        return getAsync().joinInternal();
    }

    @Override
    public void set(T newValue) {
        setAsync(newValue).joinInternal();
    }

    @Override
    public T getAndSet(T newValue) {
        return getAndSetAsync(newValue).joinInternal();
    }

    @Override
    public boolean isNull() {
        return isNullAsync().joinInternal();
    }

    @Override
    public void clear() {
        clearAsync().joinInternal();
    }

    @Override
    public boolean contains(T value) {
        return containsAsync(value).joinInternal();
    }

    @Override
    public void alter(IFunction<T, T> function) {
        alterAsync(function).joinInternal();
    }

    @Override
    public T alterAndGet(IFunction<T, T> function) {
        return alterAndGetAsync(function).joinInternal();
    }

    @Override
    public T getAndAlter(IFunction<T, T> function) {
        return getAndAlterAsync(function).joinInternal();
    }

    @Override
    public <R> R apply(IFunction<T, R> function) {
        return applyAsync(function).joinInternal();
    }

    @Override
    public InternalCompletableFuture<Boolean> compareAndSetAsync(T expect, T update) {
        return invocationManager.invoke(groupId, new CompareAndSetOp(objectName, toData(expect), toData(update)));
    }

    @Override
    public InternalCompletableFuture<T> getAsync() {
        return invocationManager.query(groupId, new GetOp(objectName), LINEARIZABLE);
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(T newValue) {
        return invocationManager.invoke(groupId, new SetOp(objectName, toData(newValue), false));
    }

    @Override
    public InternalCompletableFuture<T> getAndSetAsync(T newValue) {
        return invocationManager.invoke(groupId, new SetOp(objectName, toData(newValue), true));
    }

    @Override
    public InternalCompletableFuture<Boolean> isNullAsync() {
        return containsAsync(null);
    }

    @Override
    public InternalCompletableFuture<Void> clearAsync() {
        return setAsync(null);
    }

    @Override
    public InternalCompletableFuture<Boolean> containsAsync(T expected) {
        return invocationManager.query(groupId, new ContainsOp(objectName, toData(expected)), LINEARIZABLE);
    }

    @Override
    public InternalCompletableFuture<Void> alterAsync(IFunction<T, T> function) {
        checkTrue(function != null, "Function cannot be null");
        return invocationManager.invoke(groupId, new ApplyOp(objectName, toData(function), NO_RETURN_VALUE, true));
    }

    @Override
    public InternalCompletableFuture<T> alterAndGetAsync(IFunction<T, T> function) {
        checkTrue(function != null, "Function cannot be null");
        return invocationManager.invoke(groupId, new ApplyOp(objectName, toData(function), RETURN_NEW_VALUE, true));
    }

    @Override
    public InternalCompletableFuture<T> getAndAlterAsync(IFunction<T, T> function) {
        checkTrue(function != null, "Function cannot be null");
        return invocationManager.invoke(groupId, new ApplyOp(objectName, toData(function), RETURN_OLD_VALUE, true));
    }

    @Override
    public <R> InternalCompletableFuture<R> applyAsync(IFunction<T, R> function) {
        checkTrue(function != null, "Function cannot be null");
        RaftOp op = new ApplyOp(objectName, toData(function), RETURN_NEW_VALUE, false);
        return invocationManager.query(groupId, op, LINEARIZABLE);
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
        return AtomicRefService.SERVICE_NAME;
    }

    @Override
    public void destroy() {
        invocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), objectName)).joinInternal();
    }

    public CPGroupId getGroupId() {
        return groupId;
    }

    private Data toData(Object value) {
        return serializationService.toData(value);
    }

}
