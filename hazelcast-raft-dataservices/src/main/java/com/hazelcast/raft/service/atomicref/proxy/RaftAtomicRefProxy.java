/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.atomicref.proxy;

import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.atomicref.RaftAtomicRefService;
import com.hazelcast.raft.service.atomicref.operation.ApplyOp;
import com.hazelcast.raft.service.atomicref.operation.CompareAndSetOp;
import com.hazelcast.raft.service.atomicref.operation.ContainsOp;
import com.hazelcast.raft.service.atomicref.operation.GetOp;
import com.hazelcast.raft.service.atomicref.operation.SetOp;
import com.hazelcast.raft.service.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.serialization.SerializationService;

import static com.hazelcast.raft.service.atomicref.operation.ApplyOp.ReturnValueType.NO_RETURN_VALUE;
import static com.hazelcast.raft.service.atomicref.operation.ApplyOp.ReturnValueType.RETURN_NEW_VALUE;
import static com.hazelcast.raft.service.atomicref.operation.ApplyOp.ReturnValueType.RETURN_OLD_VALUE;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * Raft-based proxy implementation of {@link IAtomicReference} interface
 *
 * @param <T> type of the referenced values
 */
public class RaftAtomicRefProxy<T> implements IAtomicReference<T> {

    private final String name;
    private final RaftGroupId groupId;
    private final RaftInvocationManager invocationManager;
    private final SerializationService serializationService;

    public RaftAtomicRefProxy(RaftInvocationManager invocationManager, SerializationService serializationService,
                              RaftGroupId groupId, String name) {
        this.name = name;
        this.groupId = groupId;
        this.invocationManager = invocationManager;
        this.serializationService = serializationService;
    }

    @Override
    public boolean compareAndSet(T expect, T update) {
        return compareAndSetAsync(expect, update).join();
    }

    @Override
    public T get() {
        return getAsync().join();
    }

    @Override
    public void set(T newValue) {
        setAsync(newValue).join();
    }

    @Override
    public T getAndSet(T newValue) {
        return getAndSetAsync(newValue).join();
    }

    @Override
    public T setAndGet(T update) {
        set(update);
        return update;
    }

    @Override
    public boolean isNull() {
        return isNullAsync().join();
    }

    @Override
    public void clear() {
        clearAsync().join();
    }

    @Override
    public boolean contains(T value) {
        return containsAsync(value).join();
    }

    @Override
    public void alter(IFunction<T, T> function) {
        alterAsync(function).join();
    }

    @Override
    public T alterAndGet(IFunction<T, T> function) {
        return alterAndGetAsync(function).join();
    }

    @Override
    public T getAndAlter(IFunction<T, T> function) {
        return getAndAlterAsync(function).join();
    }

    @Override
    public <R> R apply(IFunction<T, R> function) {
        return applyAsync(function).join();
    }

    @Override
    public InternalCompletableFuture<Boolean> compareAndSetAsync(T expect, T update) {
        return invocationManager.invoke(groupId, new CompareAndSetOp(name, toData(expect), toData(update)));
    }

    @Override
    public InternalCompletableFuture<T> getAsync() {
        return invocationManager.invoke(groupId, new GetOp(name));
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(T newValue) {
        return invocationManager.invoke(groupId, new SetOp(name, toData(newValue), false));
    }

    @Override
    public InternalCompletableFuture<T> getAndSetAsync(T newValue) {
        return invocationManager.invoke(groupId, new SetOp(name, toData(newValue), true));
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
        return invocationManager.invoke(groupId, new ContainsOp(name, toData(expected)));
    }

    @Override
    public InternalCompletableFuture<Void> alterAsync(IFunction<T, T> function) {
        checkTrue(function != null, "Function cannot be null");
        return invocationManager.invoke(groupId, new ApplyOp(name, toData(function), NO_RETURN_VALUE, true));
    }

    @Override
    public InternalCompletableFuture<T> alterAndGetAsync(IFunction<T, T> function) {
        checkTrue(function != null, "Function cannot be null");
        return invocationManager.invoke(groupId, new ApplyOp(name, toData(function), RETURN_NEW_VALUE, true));
    }

    @Override
    public InternalCompletableFuture<T> getAndAlterAsync(IFunction<T, T> function) {
        checkTrue(function != null, "Function cannot be null");
        return invocationManager.invoke(groupId, new ApplyOp(name, toData(function), RETURN_OLD_VALUE, true));
    }

    @Override
    public <R> InternalCompletableFuture<R> applyAsync(IFunction<T, R> function) {
        checkTrue(function != null, "Function cannot be null");
        return invocationManager.invoke(groupId, new ApplyOp(name, toData(function), RETURN_NEW_VALUE, false));
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return RaftAtomicRefService.SERVICE_NAME;
    }

    @Override
    public void destroy() {
        invocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), name)).join();
    }

    public RaftGroupId getGroupId() {
        return groupId;
    }

    private Data toData(Object value) {
        return serializationService.toData(value);
    }

}
