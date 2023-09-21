/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cp.internal.datastructures.atomiclong;

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAddAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongApplyCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAddCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetCodec;
import com.hazelcast.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLongService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.UUID;

import static com.hazelcast.cp.internal.datastructures.atomiclong.operation.AlterOp.AlterResultType.NEW_VALUE;
import static com.hazelcast.cp.internal.datastructures.atomiclong.operation.AlterOp.AlterResultType.OLD_VALUE;

/**
 * Client-side Raft-based proxy implementation of {@link IAtomicLong}
 */
@SuppressWarnings("checkstyle:methodcount")
public class AtomicLongProxy extends ClientProxy implements IAtomicLong {

    private final RaftGroupId groupId;
    private final String objectName;
    private UUID objectUUID;

    public AtomicLongProxy(ClientContext context, RaftGroupId groupId, String proxyName, String objectName, UUID objectUUID) {
        super(AtomicLongService.SERVICE_NAME, proxyName, context);
        this.groupId = groupId;
        this.objectName = objectName;
        this.objectUUID = objectUUID;
    }

    @Override
    public long addAndGet(long delta) {
        return addAndGetAsync(delta).joinInternal();
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        return compareAndSetAsync(expect, update).joinInternal();
    }

    @Override
    public long decrementAndGet() {
        return decrementAndGetAsync().joinInternal();
    }

    @Override
    public long getAndDecrement() {
        return getAndDecrementAsync().joinInternal();
    }

    @Override
    public long get() {
        return getAsync().joinInternal();
    }

    @Override
    public long getAndAdd(long delta) {
        return getAndAddAsync(delta).joinInternal();
    }

    @Override
    public long getAndSet(long newValue) {
        return getAndSetAsync(newValue).joinInternal();
    }

    @Override
    public long incrementAndGet() {
        return incrementAndGetAsync().joinInternal();
    }

    @Override
    public long getAndIncrement() {
        return getAndIncrementAsync().joinInternal();
    }

    @Override
    public void set(long newValue) {
        setAsync(newValue).joinInternal();
    }

    @Override
    public void alter(IFunction<Long, Long> function) {
        alterAsync(function).joinInternal();
    }

    @Override
    public long alterAndGet(IFunction<Long, Long> function) {
        return alterAndGetAsync(function).joinInternal();
    }

    @Override
    public long getAndAlter(IFunction<Long, Long> function) {
        return getAndAlterAsync(function).joinInternal();
    }

    @Override
    public <R> R apply(IFunction<Long, R> function) {
        return applyAsync(function).joinInternal();
    }

    @Override
    public InternalCompletableFuture<Long> addAndGetAsync(long delta) {
        ClientMessage request = AtomicLongAddAndGetCodec.encodeRequest(groupId, getCombinedObjectName(), delta);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(), AtomicLongAddAndGetCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<Boolean> compareAndSetAsync(long expect, long update) {
        ClientMessage request = AtomicLongCompareAndSetCodec.encodeRequest(groupId, getCombinedObjectName(), expect, update);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(), AtomicLongCompareAndSetCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<Long> decrementAndGetAsync() {
        return addAndGetAsync(-1);
    }

    @Override
    public InternalCompletableFuture<Long> getAndDecrementAsync() {
        return getAndAddAsync(-1);
    }

    @Override
    public InternalCompletableFuture<Long> getAsync() {
        ClientMessage request = AtomicLongGetCodec.encodeRequest(groupId, getCombinedObjectName());
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(), AtomicLongGetCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<Long> getAndAddAsync(long delta) {
        ClientMessage request = AtomicLongGetAndAddCodec.encodeRequest(groupId, getCombinedObjectName(), delta);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(), AtomicLongGetAndAddCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<Long> getAndSetAsync(long newValue) {
        ClientMessage request = AtomicLongGetAndSetCodec.encodeRequest(groupId, getCombinedObjectName(), newValue);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(), AtomicLongGetAndSetCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<Long> incrementAndGetAsync() {
        return addAndGetAsync(1);
    }

    @Override
    public InternalCompletableFuture<Long> getAndIncrementAsync() {
        return getAndAddAsync(1);
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(long newValue) {
        return (InternalCompletableFuture) getAndSetAsync(newValue);
    }

    @Override
    public InternalCompletableFuture<Void> alterAsync(IFunction<Long, Long> function) {
        return (InternalCompletableFuture) alterAndGetAsync(function);
    }

    @Override
    public InternalCompletableFuture<Long> alterAndGetAsync(IFunction<Long, Long> function) {
        Data f = getSerializationService().toData(function);
        ClientMessage request = AtomicLongAlterCodec.encodeRequest(groupId, getCombinedObjectName(), f, NEW_VALUE.value());
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(), AtomicLongAlterCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<Long> getAndAlterAsync(IFunction<Long, Long> function) {
        Data f = getSerializationService().toData(function);
        ClientMessage request = AtomicLongAlterCodec.encodeRequest(groupId, getCombinedObjectName(), f, OLD_VALUE.value());
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(), AtomicLongAlterCodec::decodeResponse);
    }

    @Override
    public <R> InternalCompletableFuture<R> applyAsync(IFunction<Long, R> function) {
        Data f = getSerializationService().toData(function);
        ClientMessage request = AtomicLongApplyCodec.encodeRequest(groupId, getCombinedObjectName(), f);
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<>(future, getSerializationService(), AtomicLongApplyCodec::decodeResponse);
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onDestroy() {
        ClientMessage request = CPGroupDestroyCPObjectCodec.encodeRequest(groupId, getServiceName(), getCombinedObjectName());
        new ClientInvocation(getClient(), request, name).invoke().joinInternal();
    }

    public CPGroupId getGroupId() {
        return groupId;
    }

    private String getCombinedObjectName() {
        return objectName + "@" + objectUUID;
    }
}
