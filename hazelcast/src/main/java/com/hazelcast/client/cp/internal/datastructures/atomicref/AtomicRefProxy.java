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

package com.hazelcast.client.cp.internal.datastructures.atomicref;

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.AtomicRefApplyCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefContainsCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefSetCodec;
import com.hazelcast.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefService;
import com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp.ReturnValueType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import static com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp.ReturnValueType.NO_RETURN_VALUE;
import static com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp.ReturnValueType.RETURN_NEW_VALUE;
import static com.hazelcast.cp.internal.datastructures.atomicref.operation.ApplyOp.ReturnValueType.RETURN_OLD_VALUE;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * Client-side Raft-based proxy implementation of {@link IAtomicReference}
 *
 * @param <T> the type of object referred to by this reference
 */
@SuppressWarnings("checkstyle:methodcount")
public class AtomicRefProxy<T> extends ClientProxy implements IAtomicReference<T> {

    private final RaftGroupId groupId;
    private final String objectName;

    public AtomicRefProxy(ClientContext context, RaftGroupId groupId, String proxyName, String objectName) {
        super(AtomicRefService.SERVICE_NAME, proxyName, context);
        this.groupId = groupId;
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
        Data expectedData = getContext().getSerializationService().toData(expect);
        Data newData = getContext().getSerializationService().toData(update);
        ClientMessage request = AtomicRefCompareAndSetCodec.encodeRequest(groupId, objectName, expectedData, newData);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(), AtomicRefCompareAndSetCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<T> getAsync() {
        ClientMessage request = AtomicRefGetCodec.encodeRequest(groupId, objectName);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(), AtomicRefGetCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(T newValue) {
        Data data = getContext().getSerializationService().toData(newValue);
        ClientMessage request = AtomicRefSetCodec.encodeRequest(groupId, objectName, data, false);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(), AtomicRefSetCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<T> getAndSetAsync(T newValue) {
        Data data = getContext().getSerializationService().toData(newValue);
        ClientMessage request = AtomicRefSetCodec.encodeRequest(groupId, objectName, data, true);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(), AtomicRefSetCodec::decodeResponse);
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
        Data data = getContext().getSerializationService().toData(expected);
        ClientMessage request = AtomicRefContainsCodec.encodeRequest(groupId, objectName, data);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(), AtomicRefContainsCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<Void> alterAsync(IFunction<T, T> function) {
        return invokeApply(function, NO_RETURN_VALUE, true);
    }

    @Override
    public InternalCompletableFuture<T> alterAndGetAsync(IFunction<T, T> function) {
        return invokeApply(function, RETURN_NEW_VALUE, true);
    }

    @Override
    public InternalCompletableFuture<T> getAndAlterAsync(IFunction<T, T> function) {
        return invokeApply(function, RETURN_OLD_VALUE, true);
    }

    @Override
    public <R> InternalCompletableFuture<R> applyAsync(IFunction<T, R> function) {
        return invokeApply(function, RETURN_NEW_VALUE, false);
    }

    @Override
    public void onDestroy() {
        ClientMessage request = CPGroupDestroyCPObjectCodec.encodeRequest(groupId, getServiceName(), objectName);
        new ClientInvocation(getClient(), request, name).invoke().joinInternal();
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    public CPGroupId getGroupId() {
        return groupId;
    }

    private <T2, T3> InternalCompletableFuture<T3> invokeApply(IFunction<T, T2> function, ReturnValueType returnValueType,
                                                               boolean alter) {
        checkTrue(function != null, "Function cannot be null");
        Data data = getContext().getSerializationService().toData(function);
        ClientMessage request = AtomicRefApplyCodec.encodeRequest(groupId, objectName, data, returnValueType.value(), alter);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(), AtomicRefApplyCodec::decodeResponse);
    }

}
